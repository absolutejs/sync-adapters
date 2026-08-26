import AbsoluteDevicesCapacitor
import CryptoKit
import Foundation
import SQLite3

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

private final class NoRedirect: NSObject, URLSessionTaskDelegate {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse, newRequest request: URLRequest, completionHandler: @escaping (URLRequest?) -> Void) { completionHandler(nil) }
}

private struct NativeMutation { let id: String; let attempts: Int }
private struct NativeBatch { let body: [String: Any]; let mutations: [String: NativeMutation]; let pulls: [String: [String: Any]] }

enum AbsoluteBackgroundSyncEngine {
    private static let refreshKey = "absolutejs.auth.oidc.refresh"
    private static let dataKeyName = "absolutejs.sync.data-key.v1"
    private static let maxResponse = 5 * 1024 * 1024

    static func record(_ error: Error) {
        let defaults = AbsoluteBackgroundSyncPlugin.defaults
        defaults.set(false, forKey: "absolutejs.background-sync.running")
        defaults.set(error.localizedDescription, forKey: "absolutejs.background-sync.lastError")
    }
    static func run() async throws {
        guard let config = try AbsoluteBackgroundSyncPlugin.configuration() else { return }
        let defaults = AbsoluteBackgroundSyncPlugin.defaults
        defaults.set(true, forKey: "absolutejs.background-sync.running"); defaults.removeObject(forKey: "absolutejs.background-sync.lastError")
        let access = try await refreshAccess(config)
        let documents = try FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
        var handle: OpaquePointer?
        guard sqlite3_open_v2(documents.appendingPathComponent("\(config.databaseName)SQLite.db").path, &handle, SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX, nil) == SQLITE_OK, let db = handle else { throw SyncNativeError.invalidResponse("Unable to open Sync database.") }
        defer { sqlite3_close(db) }
        sqlite3_busy_timeout(db, 5_000)
        let batch = try prepare(db, config)
        let response = try await jsonRequest(config.endpoint, method: "POST", body: batch.body, bearer: access, issuer: config.issuer)
        let counts = try settle(db, config, batch, response)
        defaults.set(false, forKey: "absolutejs.background-sync.running"); defaults.set(Date().timeIntervalSince1970 * 1000, forKey: "absolutejs.background-sync.lastRunAt")
        defaults.set(counts.0, forKey: "absolutejs.background-sync.lastAcknowledged"); defaults.set(counts.1, forKey: "absolutejs.background-sync.lastPulled")
    }

    private static func refreshAccess(_ config: AbsoluteBackgroundSyncConfiguration) async throws -> String {
        guard let lease = AbsoluteSecureStorageVault.acquireLease(refreshKey, ttlMilliseconds: 120_000) else { throw SyncNativeError.invalidResponse("Credential refresh is already running.") }
        defer { AbsoluteSecureStorageVault.releaseLease(refreshKey, leaseId: lease) }
        guard let refresh = try AbsoluteSecureStorageVault.get(refreshKey) else {
            AbsoluteBackgroundSyncPlugin.defaults.set(false, forKey: "absolutejs.background-sync.running")
            throw SyncNativeError.noSession
        }
        let discoveryURL = config.issuer.appendingPathComponent(".well-known/openid-configuration")
        let discovery = try await jsonRequest(discoveryURL, method: "GET", body: nil, bearer: nil, issuer: config.issuer)
        guard let endpointValue = discovery["token_endpoint"] as? String, let endpoint = URL(string: endpointValue) else { throw SyncNativeError.invalidResponse("OIDC discovery has no token endpoint.") }
        let form = ["client_id": config.clientId, "grant_type": "refresh_token", "refresh_token": refresh, "resource": config.issuer.absoluteString]
            .map { key, value in "\(encode(key))=\(encode(value))" }.joined(separator: "&").data(using: .utf8)!
        let tokens = try await jsonRequest(endpoint, method: "POST", rawBody: form, contentType: "application/x-www-form-urlencoded", bearer: nil, issuer: config.issuer, advertisedTokenEndpoint: true)
        guard let replacement = tokens["refresh_token"] as? String, !replacement.isEmpty,
              let access = tokens["access_token"] as? String else { throw SyncNativeError.invalidResponse("OIDC token response is malformed.") }
        guard try AbsoluteSecureStorageVault.setIfLease(refreshKey, value: replacement, leaseId: lease) else {
            AbsoluteBackgroundSyncPlugin.defaults.set(false, forKey: "absolutejs.background-sync.running")
            throw SyncNativeError.noSession
        }
        return access
    }
    private static func encode(_ value: String) -> String { value.addingPercentEncoding(withAllowedCharacters: .alphanumerics) ?? "" }

    private static func prepare(_ db: OpaquePointer, _ config: AbsoluteBackgroundSyncConfiguration) throws -> NativeBatch {
        try execute(db, "BEGIN IMMEDIATE")
        do {
            var mutations: [[String: Any]] = [], mutationMap: [String: NativeMutation] = [:]
            for row in try rows(db, "SELECT operation_id,record_json FROM absolute_sync_mutations WHERE namespace=? ORDER BY created_at,operation_id", [config.namespace]) where mutations.count < config.maxMutations {
                let id = row[0], record = try decodeRecord(row[1], kind: "mutation", namespace: config.namespace); let now = Date().timeIntervalSince1970 * 1000
                if record["state"] as? String == "dead-letter" || (record["nextAttemptAt"] as? NSNumber)?.doubleValue ?? 0 > now { continue }
                let attempts = ((record["attempts"] as? NSNumber)?.intValue ?? 0) + 1; var next = record; next["attempts"] = attempts; next.removeValue(forKey: "nextAttemptAt")
                try updateRecord(db, "absolute_sync_mutations", "operation_id", "mutation", config.namespace, id, next)
				guard let name = record["name"] as? String else { throw SyncNativeError.invalidResponse("Local mutation has no name.") }
                var mutation: [String: Any] = ["operationId": id, "name": name]; if let args = record["args"] { mutation["args"] = args }
                mutations.append(mutation); mutationMap[id] = NativeMutation(id: id, attempts: attempts)
            }
            var pulls: [[String: Any]] = [], pullMap: [String: [String: Any]] = [:]
            for row in try rows(db, "SELECT collection_key,record_json FROM absolute_sync_collections WHERE namespace=? ORDER BY collection_key", [config.namespace]) where pulls.count < config.maxPulls {
                let key = row[0], record = try decodeRecord(row[1], kind: "collection", namespace: config.namespace); guard let collection = record["collection"] as? String, !collection.isEmpty else { continue }
                var pull: [String: Any] = ["id": key, "collection": collection]; if let params = record["params"] { pull["params"] = params }
                if record["headlessKey"] as? String == "id" { if let cursor = record["cursor"] { pull["since"] = cursor } else if ((record["version"] as? NSNumber)?.intValue ?? 0) > 0 { pull["since"] = record["version"] } }
                pulls.append(pull); pullMap[key] = record
            }
            try execute(db, "COMMIT")
            return NativeBatch(body: ["version": 1, "mutations": mutations, "pulls": pulls], mutations: mutationMap, pulls: pullMap)
        } catch { try? execute(db, "ROLLBACK"); throw error }
    }

    private static func settle(_ db: OpaquePointer, _ config: AbsoluteBackgroundSyncConfiguration, _ batch: NativeBatch, _ response: [String: Any]) throws -> (Int, Int) {
        guard (response["version"] as? NSNumber)?.intValue == 1 else { throw SyncNativeError.invalidResponse("Unsupported Headless Sync response.") }
        let mutationResults = index(response["mutations"], "operationId"), pullResults = index(response["pulls"], "id")
        var acknowledged = 0, pulled = 0; try execute(db, "BEGIN IMMEDIATE")
        do {
            for sent in batch.mutations.values {
                guard var current = try record(db, "absolute_sync_mutations", "operation_id", "mutation", config.namespace, sent.id), (current["attempts"] as? NSNumber)?.intValue == sent.attempts else { continue }
                if mutationResults[sent.id]?["status"] as? String == "ack" { try execute(db, "DELETE FROM absolute_sync_mutations WHERE namespace=? AND operation_id=?", [config.namespace, sent.id]); acknowledged += 1; continue }
                let rejection = mutationResults[sent.id]?["rejection"] as? [String: Any] ?? ["kind": "retryable", "message": "Missing mutation response."]
                current["lastError"] = rejection["message"]; current["rejection"] = rejection
                let conflictPolicy = current["conflictPolicy"] as? [String: Any]
                let conflictStrategy = conflictPolicy?["strategy"] as? String ?? "manual"
                if rejection["kind"] as? String == "conflict" && conflictStrategy == "server-wins" { try execute(db, "DELETE FROM absolute_sync_mutations WHERE namespace=? AND operation_id=?", [config.namespace, sent.id]); continue }
                let conflictAttempts = (current["conflictAttempts"] as? NSNumber)?.intValue ?? 0
                let maxConflictAttempts = (conflictPolicy?["maxAttempts"] as? NSNumber)?.intValue ?? 1
                if rejection["kind"] as? String == "conflict" && conflictStrategy == "client-wins" && conflictAttempts < maxConflictAttempts { current["conflictAttempts"] = conflictAttempts + 1; current["state"] = "pending"; current.removeValue(forKey: "nextAttemptAt"); try updateRecord(db, "absolute_sync_mutations", "operation_id", "mutation", config.namespace, sent.id, current); continue }
                if rejection["kind"] as? String != "retryable" || sent.attempts >= config.maxAttempts { current["state"] = "dead-letter"; current["deadLetteredAt"] = Date().timeIntervalSince1970 * 1000; current.removeValue(forKey: "nextAttemptAt") }
                else { let hint = (rejection["retryAfterMs"] as? NSNumber)?.doubleValue ?? min(30_000, 500 * pow(2, Double(max(0, sent.attempts - 1)))); current["state"] = "pending"; current["nextAttemptAt"] = Date().timeIntervalSince1970 * 1000 + max(0, hint) }
                try updateRecord(db, "absolute_sync_mutations", "operation_id", "mutation", config.namespace, sent.id, current)
            }
            for (key, baseline) in batch.pulls {
                guard let result = pullResults[key], result["type"] as? String != "error", let current = try record(db, "absolute_sync_collections", "collection_key", "collection", config.namespace, key), same(current, baseline) else { continue }
                try updateRecord(db, "absolute_sync_collections", "collection_key", "collection", config.namespace, key, try applyPull(current, result)); pulled += 1
            }
            try execute(db, "COMMIT"); return (acknowledged, pulled)
        } catch { try? execute(db, "ROLLBACK"); throw error }
    }

    private static func applyPull(_ current: [String: Any], _ result: [String: Any]) throws -> [String: Any] {
        var next = current
        if result["type"] as? String == "snapshot" { next["rows"] = result["rows"] }
        else {
            var keyed: [String: [String: Any]] = [:]; for row in current["rows"] as? [[String: Any]] ?? [] { keyed[try key(row)] = row }
            for row in result["removed"] as? [[String: Any]] ?? [] { keyed.removeValue(forKey: try key(row)) }
            for row in (result["changed"] as? [[String: Any]] ?? []) + (result["added"] as? [[String: Any]] ?? []) { keyed[try key(row)] = row }
            next["rows"] = Array(keyed.values)
        }
        next["version"] = result["version"]; next["cursor"] = result["cursor"]; return next
    }
    private static func key(_ row: [String: Any]) throws -> String { guard let id = row["id"], id is String || id is NSNumber else { throw SyncNativeError.invalidResponse("Background Sync row requires id.") }; return "\(type(of: id)):\(id)" }
    private static func same(_ a: [String: Any], _ b: [String: Any]) -> Bool { ((a["version"] as? NSNumber)?.intValue ?? 0) == ((b["version"] as? NSNumber)?.intValue ?? 0) && (a["cursor"] as? String ?? "") == (b["cursor"] as? String ?? "") }
    private static func index(_ value: Any?, _ field: String) -> [String: [String: Any]] { var result: [String: [String: Any]] = [:]; for item in value as? [[String: Any]] ?? [] { if let key = item[field] as? String { result[key] = item } }; return result }

    private static func object(_ json: String) throws -> [String: Any] { guard let value = try JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any] else { throw SyncNativeError.invalidResponse("Invalid local Sync record.") }; return value }
    private static func dataKey() throws -> SymmetricKey? { guard let value = try AbsoluteSecureStorageVault.get(dataKeyName) else { return nil }; guard let bytes = Data(base64Encoded: value), bytes.count == 32 else { throw SyncNativeError.invalidResponse("Protected Sync data key is invalid.") }; return SymmetricKey(data: bytes) }
    private static func aad(_ kind: String, _ namespace: String, _ name: String) -> Data { Data("absolute-sync-v1\u{0}\(kind)\u{0}\(namespace)\u{0}\(name)".utf8) }
    private static func decodeRecord(_ stored: String, kind: String, namespace: String) throws -> [String: Any] { let outer = try object(stored); guard let envelope = outer["__absoluteSyncProtected"] as? [String: Any] else { return outer }; guard envelope["protector"] as? String == "aes-256-gcm-v1", let name = envelope["name"] as? String, let encoded = envelope["value"] as? String, let packed = Data(base64Encoded: encoded), packed.count >= 28, let key = try dataKey() else { throw SyncNativeError.invalidResponse("Protected Sync data key is unavailable.") }; let nonce = try AES.GCM.Nonce(data: packed.prefix(12)), ciphertextAndTag = packed.dropFirst(12), box = try AES.GCM.SealedBox(nonce: nonce, ciphertext: ciphertextAndTag.dropLast(16), tag: ciphertextAndTag.suffix(16)); return try object(String(data: try AES.GCM.open(box, using: key, authenticating: aad(kind, namespace, name)), encoding: .utf8)!) }
    private static func encodeRecord(_ value: [String: Any], kind: String, namespace: String, name: String) throws -> String { let plain = try JSONSerialization.data(withJSONObject: value); guard let key = try dataKey() else { return String(data: plain, encoding: .utf8)! }; let sealed = try AES.GCM.seal(plain, using: key, authenticating: aad(kind, namespace, name)); var packed = Data(sealed.nonce); packed.append(sealed.ciphertext); packed.append(sealed.tag); let envelope: [String: Any] = ["__absoluteSyncProtected": ["name": name, "protector": "aes-256-gcm-v1", "value": packed.base64EncodedString()]]; return String(data: try JSONSerialization.data(withJSONObject: envelope), encoding: .utf8)! }
    private static func record(_ db: OpaquePointer, _ table: String, _ field: String, _ kind: String, _ namespace: String, _ id: String) throws -> [String: Any]? { guard let value = try rows(db, "SELECT record_json FROM \(table) WHERE namespace=? AND \(field)=? LIMIT 1", [namespace,id]).first?.first else { return nil }; return try decodeRecord(value, kind: kind, namespace: namespace) }
    private static func updateRecord(_ db: OpaquePointer, _ table: String, _ field: String, _ kind: String, _ namespace: String, _ id: String, _ value: [String: Any]) throws { let name = (value[kind == "mutation" ? "name" : "collection"] as? String) ?? id; let json = try encodeRecord(value, kind: kind, namespace: namespace, name: name); try execute(db, "UPDATE \(table) SET record_json=? WHERE namespace=? AND \(field)=?", [json,namespace,id]) }
    private static func rows(_ db: OpaquePointer, _ sql: String, _ bindings: [String]) throws -> [[String]] { var statement: OpaquePointer?; guard sqlite3_prepare_v2(db,sql,-1,&statement,nil)==SQLITE_OK else { throw SyncNativeError.invalidResponse("SQLite prepare failed.") }; defer{sqlite3_finalize(statement)}; for (i,value) in bindings.enumerated(){sqlite3_bind_text(statement,Int32(i+1),(value as NSString).utf8String,-1,SQLITE_TRANSIENT)}; var output:[[String]]=[]; while sqlite3_step(statement)==SQLITE_ROW { output.append((0..<sqlite3_column_count(statement)).map{String(cString:sqlite3_column_text(statement,$0))}) }; return output }
    private static func execute(_ db: OpaquePointer, _ sql: String, _ bindings: [String] = []) throws { var statement: OpaquePointer?; guard sqlite3_prepare_v2(db,sql,-1,&statement,nil)==SQLITE_OK else { throw SyncNativeError.invalidResponse("SQLite prepare failed.") }; defer{sqlite3_finalize(statement)}; for(i,value)in bindings.enumerated(){sqlite3_bind_text(statement,Int32(i+1),(value as NSString).utf8String,-1,SQLITE_TRANSIENT)}; guard sqlite3_step(statement)==SQLITE_DONE else { throw SyncNativeError.invalidResponse("SQLite write failed.") } }

    private static func jsonRequest(_ url: URL, method: String, body: [String: Any]?, bearer: String?, issuer: URL) async throws -> [String: Any] { try await jsonRequest(url, method: method, rawBody: body.map{try! JSONSerialization.data(withJSONObject:$0)}, contentType: body == nil ? nil : "application/json", bearer: bearer, issuer: issuer) }
    private static func jsonRequest(_ url: URL, method: String, rawBody: Data?, contentType: String?, bearer: String?, issuer: URL, advertisedTokenEndpoint: Bool = false) async throws -> [String: Any] {
        guard (rawBody?.count ?? 0) <= maxResponse else { throw SyncNativeError.invalidResponse("Background request is too large.") }
        if advertisedTokenEndpoint {
            let loopback = url.scheme == "http" && ["localhost", "127.0.0.1", "::1"].contains(url.host?.lowercased() ?? "")
            guard (url.scheme == "https" || loopback), url.user == nil, url.password == nil, url.fragment == nil else { throw SyncNativeError.invalidResponse("OIDC advertised an unsafe token endpoint.") }
        } else {
            guard AbsoluteBackgroundSyncConfiguration.origin(url)==AbsoluteBackgroundSyncConfiguration.origin(issuer) else { throw SyncNativeError.invalidResponse("Background request left the Auth issuer origin.") }
        }
        var request=URLRequest(url:url);request.httpMethod=method;request.httpBody=rawBody;request.timeoutInterval=20;request.setValue("application/json",forHTTPHeaderField:"Accept");if let contentType{request.setValue(contentType,forHTTPHeaderField:"Content-Type")};if let bearer{request.setValue("Bearer \(bearer)",forHTTPHeaderField:"Authorization")}
        let session=URLSession(configuration:.ephemeral,delegate:NoRedirect(),delegateQueue:nil);let(data,response)=try await session.data(for:request);guard data.count<=maxResponse,let http=response as? HTTPURLResponse,(200..<300).contains(http.statusCode),let value=try JSONSerialization.jsonObject(with:data)as?[String:Any]else{throw SyncNativeError.invalidResponse("Background request failed.")};return value
    }
}

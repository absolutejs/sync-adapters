import BackgroundTasks
import Capacitor
import Foundation

private let taskSuffix = ".absolutejs.background-sync"

struct AbsoluteBackgroundSyncConfiguration: Codable {
    let endpoint: URL
    let issuer: URL
    let clientId: String
    let namespace: String
    let databaseName: String
    let intervalMinutes: Int
    let maxMutations: Int
    let maxPulls: Int
    let maxAttempts: Int

    init(call: CAPPluginCall) throws {
        guard let endpointValue = call.getString("endpoint"), let endpoint = URL(string: endpointValue),
              let issuerValue = call.getString("issuer"), let issuer = URL(string: issuerValue),
              let clientId = call.getString("clientId"), !clientId.isEmpty,
              let namespace = call.getString("namespace"), !namespace.isEmpty else { throw SyncNativeError.invalidConfiguration }
        guard Self.origin(endpoint) == Self.origin(issuer), endpoint.user == nil, endpoint.password == nil,
              issuer.path.isEmpty || issuer.path == "/", issuer.query == nil, issuer.fragment == nil else { throw SyncNativeError.invalidConfiguration }
		let loopback = issuer.scheme == "http" && ["localhost", "127.0.0.1", "::1"].contains(issuer.host?.lowercased() ?? "")
		guard issuer.scheme == "https" || loopback else { throw SyncNativeError.invalidConfiguration }
        let databaseName = call.getString("databaseName") ?? "absolutejs-sync-local-v1"
        guard databaseName.range(of: "^[A-Za-z0-9._-]{1,128}$", options: .regularExpression) != nil else { throw SyncNativeError.invalidConfiguration }
        let interval = call.getInt("intervalMinutes") ?? 15
        let mutationLimit = call.getInt("maxMutations") ?? 50
        let pullLimit = call.getInt("maxPulls") ?? 50
        let attemptLimit = call.getInt("maxAttempts") ?? 5
        guard interval >= 15 && interval <= 1440, mutationLimit >= 0 && mutationLimit <= 100, pullLimit >= 0 && pullLimit <= 100, attemptLimit >= 1 && attemptLimit <= 100 else { throw SyncNativeError.invalidConfiguration }
        self.endpoint = endpoint; self.issuer = issuer; self.clientId = clientId; self.namespace = namespace
        self.databaseName = databaseName; intervalMinutes = interval; maxMutations = mutationLimit; maxPulls = pullLimit; maxAttempts = attemptLimit
    }

    static func origin(_ url: URL) -> String {
        let port = url.port ?? (url.scheme == "https" ? 443 : 80)
        return "\(url.scheme ?? "")://\(url.host?.lowercased() ?? ""):\(port)"
    }
}

enum SyncNativeError: Error { case invalidConfiguration, noSession, invalidResponse(String) }

@objc(AbsoluteBackgroundSyncPlugin)
public class AbsoluteBackgroundSyncPlugin: CAPPlugin, CAPBridgedPlugin {
    public let identifier = "AbsoluteBackgroundSyncPlugin"
    public let jsName = "AbsoluteBackgroundSync"
    public let pluginMethods: [CAPPluginMethod] = [
        CAPPluginMethod(name: "configure", returnType: CAPPluginReturnPromise),
        CAPPluginMethod(name: "clear", returnType: CAPPluginReturnPromise),
        CAPPluginMethod(name: "runNow", returnType: CAPPluginReturnPromise),
        CAPPluginMethod(name: "status", returnType: CAPPluginReturnPromise),
    ]
    static var taskIdentifier: String { "\(Bundle.main.bundleIdentifier ?? "absolutejs.app")\(taskSuffix)" }
    static let defaults = UserDefaults.standard
    static let configKey = "absolutejs.background-sync.config"

    /** AbsoluteJS inserts this call in AppDelegate before launch completes. */
    @objc public static func registerBackgroundTask() {
        BGTaskScheduler.shared.register(forTaskWithIdentifier: taskIdentifier, using: nil) { task in
            guard let processing = task as? BGProcessingTask else { task.setTaskCompleted(success: false); return }
            let work = Task {
                do { try await AbsoluteBackgroundSyncEngine.run(); schedule(); processing.setTaskCompleted(success: true) }
                catch SyncNativeError.noSession { processing.setTaskCompleted(success: true) }
                catch { AbsoluteBackgroundSyncEngine.record(error); schedule(); processing.setTaskCompleted(success: false) }
            }
            processing.expirationHandler = { work.cancel() }
        }
    }

    static func configuration() throws -> AbsoluteBackgroundSyncConfiguration? {
        guard let data = defaults.data(forKey: configKey) else { return nil }
        return try JSONDecoder().decode(AbsoluteBackgroundSyncConfiguration.self, from: data)
    }
    static func schedule() {
        guard let config = try? configuration() else { return }
        let request = BGProcessingTaskRequest(identifier: taskIdentifier)
        request.requiresNetworkConnectivity = true
        request.earliestBeginDate = Date(timeIntervalSinceNow: TimeInterval(config.intervalMinutes * 60))
        try? BGTaskScheduler.shared.submit(request)
    }
    static func statusValue() -> [String: Any] {
        var value: [String: Any] = ["configured": defaults.data(forKey: configKey) != nil, "running": defaults.bool(forKey: "absolutejs.background-sync.running")]
        for key in ["lastRunAt", "lastAcknowledged", "lastPulled"] where defaults.object(forKey: "absolutejs.background-sync.\(key)") != nil { value[key] = defaults.object(forKey: "absolutejs.background-sync.\(key)")! }
        if let error = defaults.string(forKey: "absolutejs.background-sync.lastError") { value["lastError"] = error }
        return value
    }

    @objc func configure(_ call: CAPPluginCall) {
        do { let config = try AbsoluteBackgroundSyncConfiguration(call: call); Self.defaults.set(try JSONEncoder().encode(config), forKey: Self.configKey); Self.schedule(); call.resolve() }
        catch { call.reject("Invalid background Sync configuration.", "INVALID_ARGUMENT", error) }
    }
    @objc func clear(_ call: CAPPluginCall) { BGTaskScheduler.shared.cancel(taskRequestWithIdentifier: Self.taskIdentifier); Self.defaults.removeObject(forKey: Self.configKey); call.resolve() }
    @objc func runNow(_ call: CAPPluginCall) { Task { do { try await AbsoluteBackgroundSyncEngine.run(); call.resolve(Self.statusValue()) } catch SyncNativeError.noSession { Self.defaults.set(false, forKey: "absolutejs.background-sync.running"); call.resolve(Self.statusValue()) } catch { AbsoluteBackgroundSyncEngine.record(error); call.reject("Background Sync failed.", "SYNC_FAILURE", error) } } }
    @objc func status(_ call: CAPPluginCall) { call.resolve(Self.statusValue()) }
}

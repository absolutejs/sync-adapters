// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "AbsoluteSyncCapacitor",
    platforms: [.iOS(.v15)],
    products: [
        .library(name: "AbsoluteSyncCapacitor", targets: ["AbsoluteSyncCapacitor"]),
        // Capacitor 8's generated CapApp-SPM names a plugin's product from
        // its npm scope and name, so it asks for AbsolutejsSyncCapacitor.
        // The same target under that name, beside the original.
        .library(name: "AbsolutejsSyncCapacitor", targets: ["AbsoluteSyncCapacitor"])
    ],
    dependencies: [
        .package(url: "https://github.com/ionic-team/capacitor-swift-pm.git", from: "8.0.0"),
        // Both plugins are direct app dependencies. Resolving the sibling npm
        // package keeps SwiftPM on the same installed devices-capacitor build
        // that Capacitor discovered, instead of treating a monorepo subfolder
        // as a nonexistent root Swift package.
        .package(name: "AbsoluteDevicesCapacitor", path: "../devices-capacitor")
    ],
    targets: [
        .target(
            name: "AbsoluteSyncCapacitor",
            dependencies: [
                .product(name: "Capacitor", package: "capacitor-swift-pm"),
                .product(name: "AbsoluteDevicesCapacitor", package: "AbsoluteDevicesCapacitor")
            ],
            path: "ios/Sources/AbsoluteSyncCapacitor",
            linkerSettings: [.linkedLibrary("sqlite3")]
        )
    ]
)

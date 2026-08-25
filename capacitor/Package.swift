// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "AbsoluteSyncCapacitor",
    platforms: [.iOS(.v15)],
    products: [.library(name: "AbsoluteSyncCapacitor", targets: ["AbsoluteSyncCapacitor"])],
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

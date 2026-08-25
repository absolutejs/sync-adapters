Pod::Spec.new do |s|
  s.name = 'AbsoluteSyncCapacitor'
  s.version = '0.3.0'
  s.summary = 'Durable foreground and background Sync for AbsoluteJS Capacitor apps.'
  s.license = 'Apache-2.0'
  s.homepage = 'https://github.com/absolutejs/sync-adapters'
  s.author = 'Alex Kahn'
  s.source = { :git => 'https://github.com/absolutejs/sync-adapters.git', :tag => s.version.to_s }
  s.source_files = 'ios/Sources/**/*.{swift,h,m,c,cc,mm,cpp}'
  s.ios.deployment_target = '15.0'
  s.dependency 'Capacitor'
  s.dependency 'AbsoluteDevicesCapacitor'
end

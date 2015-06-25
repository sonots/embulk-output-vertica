Gem::Specification.new do |spec|
  spec.name          = "embulk-output-vertica"
  spec.version       = "0.1.0"
  spec.authors       = ["eiji.sekiya"]
  spec.email         = ["eiji.sekiya.0326@gmail.com"]
  spec.summary       = "Vertica output plugin for Embulk"
  spec.description   = "Dump records to vertica"
  spec.homepage      = "https://github.com/eratostennis/embulk-plugin-vertica"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_dependency "vertica"
  spec.add_development_dependency "bundler", "~> 1.7"
  spec.add_development_dependency "rake", "~> 10.0"
end

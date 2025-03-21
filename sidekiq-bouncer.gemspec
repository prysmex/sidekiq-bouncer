# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'sidekiq_bouncer/version'

Gem::Specification.new do |spec|
  spec.name          = 'sidekiq-bouncer'
  spec.version       = SidekiqBouncer::VERSION
  spec.authors       = ['jasonzhao6']
  spec.email         = ['jasonzhao6@gmail.com']

  spec.summary       = 'Debounce Sidekiq jobs that have the same worker class and params.'
  # spec.description   = 'Debounce Sidekiq jobs that have the same worker class and params.'
  # spec.homepage      = 'https://github.com/apartmentlist/sidekiq-bouncer'
  spec.license       = 'MIT'
  spec.required_ruby_version = '>= 3.4.2'

  # spec.metadata['homepage_uri'] = spec.homepage
  # spec.metadata['source_code_uri'] = spec.homepage
  # spec.metadata['changelog_uri'] = 'https://github.com/apartmentlist/sidekiq-bouncer/blob/master/CHANGELOG.md'
  spec.metadata['rubygems_mfa_required'] = 'true'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(__dir__) do
    `git ls-files -z`.split("\x0").reject do |f|
      (File.expand_path(f) == __FILE__) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']
end

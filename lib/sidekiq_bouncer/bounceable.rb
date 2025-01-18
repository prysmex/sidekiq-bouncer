# frozen_string_literal: true

module SidekiqBouncer
  module Bounceable

    def self.included(base)
      base.prepend InstanceMethods
      base.extend ClassMethods
    end

    module ClassMethods
      # @retrun [SidekiqBouncer::Bouncer]
      attr_reader :bouncer

      # creates and sets a +SidekiqBouncer::Bouncer+
      def register_bouncer(**)
        @bouncer = SidekiqBouncer::Bouncer.new(self, **)
      end
    end

    module InstanceMethods
      def perform(*, debounce_data, **)
        # handle non-debounced jobs and already scheduled jobs when debouncer is added for the first time
        return super(*, debounce_data, **) unless debounce_data.is_a?(Hash) && debounce_data.key?('key')

        self.class.bouncer.run(**debounce_data.symbolize_keys) do
          super(*, **)
        end
      end
    end

  end
end
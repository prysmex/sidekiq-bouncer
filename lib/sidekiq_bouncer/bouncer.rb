# frozen_string_literal: true

module SidekiqBouncer
  class Bouncer

    DELAY = 60                # Seconds
    DELAY_BUFFER = 1          # Seconds

    attr_reader :klass
    attr_accessor :delay, :delay_buffer

    # @param [Class] klass worker class that responds to `perform_at`
    # @param [Integer] delay seconds used for debouncer
    # @param [Integer] delay_buffer used to prevent a race condition that may cause a job not the be executed,
    #                  for example, if for some reason sidekiq executes the job 1 second before
    def initialize(klass, delay: DELAY, delay_buffer: DELAY_BUFFER)
      # unless klass.is_a?(Class) && klass.respond_to?(:perform_at)
      #   raise TypeError.new("first argument must be a class and respond to 'perform_at'")
      # end

      @klass = klass
      @delay = delay
      @delay_buffer = delay_buffer
    end

    # Sets the debounce key to Redis with the timestamp and schedules a job to be executed at delay + the delay_buffer,
    # adding the debounce key as the last argument so that later it can be used on execution to fetch the value on redis
    #
    # @param [*] params
    # @param [Array<Integer>|#to_s] key_or_args_indices
    # @return [TODO]
    def debounce(*params, key_or_args_indices:)
      key = case key_or_args_indices
      when Array
        params.values_at(*key_or_args_indices).join(',')
      else
        key_or_args_indices
      end

      raise TypeError.new("key must be a string, got #{key.inspect}") unless key.is_a?(String)

      key = redis_key(key)
      at = now_i + @delay

      # Add/Update the timestamp in redis with debounce delay added.
      redis.call('SET', key, at)

      # Schedule the job, adding the key as the last argument.
      @klass.perform_at(at + @delay_buffer, *params, key)
    end

    # Checks if job should be excecuted
    #
    # @param [NilClass|String] key, which was appeded by +debounce+
    # @return [False|*] false when not executed
    def run(key)
      return false unless (timestamp = let_in?(key))

      redis.call('DEL', key) unless key.nil?
      yield
    rescue StandardError => e
      redis.call('SET', key, timestamp) unless key.nil?
      raise e
    end

    # @param [NilClass|String] key
    # @return [False|Integer] Integer if should be excecuted, 1 is returned when key is nil
    def let_in?(key)
      # handle non-debounced jobs and already scheduled jobs when debouncer is added for the first time
      return 1 if key.nil?

      # Get the current value of the timestamp, set by the latest scheduled job.
      timestamp = redis.call('GET', key)

      # Another job already ran (or is running) and removed the key. TODO: this can cause
      # race conditions on jobs that are scheduled at nearly the same time since it takes time
      # to delete the key.
      return false if timestamp.nil?
      return false if now_i < timestamp.to_i # A newer job updated the key to run in the future

      timestamp
    end

    private

    # @return [RedisClient::Pooled]
    def redis
      SidekiqBouncer.config.redis_client
    end

    # Appends the job class to the key to prevent clashes
    #
    # @param [String] key
    # @return [String]
    def redis_key(key)
      "#{@klass}:#{key}"
    end

    # @return [Integer] Time#now as integer
    def now_i
      Time.now.to_i
    end

  end
end

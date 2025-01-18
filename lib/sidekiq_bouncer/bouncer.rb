# frozen_string_literal: true

require 'securerandom'

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
    # @param [String] id
    # @param [Array<Integer>|#to_s] key_or_args_indices
    # @return [TODO]
    def debounce(*params, key_or_args_indices:, id: SecureRandom.hex(3))
      key = case key_or_args_indices
      when Array
        params.values_at(*key_or_args_indices).join(',')
      else
        key_or_args_indices
      end

      raise TypeError.new("key must be a string, got #{key.inspect}") unless key.is_a?(String)

      key = redis_key(key)

      # Add/Update the id in redis with debounce delay added.
      redis.call('SET', key, id)

      # Schedule the job, adding the key as the last argument.
      @klass.perform_at(now_i + @delay + @delay_buffer, *params, {'key' => key, 'id' => id})
    end

    # Checks if job should be excecuted
    #
    # @param [String] key
    # @param [String] id
    # @return [False|*] false when not executed
    def run(key:, id:)
      redis_id = redis.call('GET', key)
      return false if redis_id != id

      redis.call('DEL', key)
      yield if block_given? # execute the job
    rescue StandardError => e
      redis.call('SET', key, redis_id) if redis_id
      raise e
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

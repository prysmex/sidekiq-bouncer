# frozen_string_literal: true

# Mock
class RedisMock # rubocop:disable Lint/EmptyClass
end

# Mock
class WorkerMock
  def self.bouncer
    SidekiqBouncer::Bouncer.new(self)
  end
end

# Tests
describe SidekiqBouncer::Bouncer do
  # careful, the bouncer instance is generally cached on the worker model
  subject(:bouncer) { WorkerMock.bouncer }

  let(:redis_client) { SidekiqBouncer.config.redis_client }
  let(:worker_klass) { WorkerMock }
  let(:now) { Time.now.to_i }

  before do
    SidekiqBouncer.configure do |config|
      config.redis_client = RedisMock.new
    end

    Timecop.freeze(Time.now)

    # stubbing
    allow(redis_client).to receive(:call)
    allow(worker_klass).to receive(:perform_at)
  end

  describe 'public methods exist' do
    it { expect(bouncer).to respond_to(:klass) }
    it { expect(bouncer).to respond_to(:delay) }
    it { expect(bouncer).to respond_to(:delay=) }
    it { expect(bouncer).to respond_to(:delay_buffer) }
    it { expect(bouncer).to respond_to(:delay_buffer=) }
    it { expect(bouncer).to respond_to(:debounce) }
    it { expect(bouncer).to respond_to(:let_in?) }
  end

  describe '.new' do
    it 'raises ArgumentError when no worker class is passed' do
      expect { described_class.new }.to raise_error(ArgumentError)
    end

    # it 'raises TypeError when first arg is not a class' do
    #   expect { described_class.new(1) }.to raise_error(TypeError)
    # end

    # it 'raises TypeError when first arg does not respond to perform_at' do
    #   expect { described_class.new(String) }.to raise_error(TypeError)
    # end

    it 'has a default value for delay' do
      expect(bouncer.delay).to eql(SidekiqBouncer::Bouncer::DELAY)
    end

    it 'has a default value for delay and delay_buffer' do
      expect(bouncer.delay_buffer).to eql(SidekiqBouncer::Bouncer::DELAY_BUFFER)
    end

    it 'supports passing delay' do
      bouncer = described_class.new(WorkerMock, delay: 10, delay_buffer: 2)
      expect(bouncer.delay).to be(10)
    end

    it 'supports passing delay_buffer' do
      bouncer = described_class.new(WorkerMock, delay: 10, delay_buffer: 2)
      expect(bouncer.delay_buffer).to be(2)
    end
  end

  describe '#debounce' do
    it 'sets scoped_key to Redis with delayed timestamp' do
      bouncer.debounce('test_param_1', 'test_param_2', key_or_args_indices: [0, 1])

      expect(redis_client)
        .to have_received(:call)
        .with('SET', 'WorkerMock:test_param_1,test_param_2', now + bouncer.delay)
    end

    it 'Calls perform_at with delay and delay_buffer, passes parameters and scoped_key' do
      bouncer.debounce('test_param_1', 'test_param_2', key_or_args_indices: [0, 1])
      expect(worker_klass).to have_received(:perform_at).with(
        now + bouncer.delay + bouncer.delay_buffer,
        'test_param_1',
        'test_param_2',
        'WorkerMock:test_param_1,test_param_2'
      )
    end

    context 'with filtered parameters by key_or_args_indices' do
      it 'sets scoped_key to Redis with delayed timestamp' do
        bouncer.debounce('test_param_1', 'test_param_2', key_or_args_indices: [0])

        expect(redis_client)
          .to have_received(:call)
          .with('SET', 'WorkerMock:test_param_1', now + bouncer.delay)
      end

      it 'Calls perform_at with delay and delay_buffer, passes parameters and scoped_key' do
        bouncer.debounce('test_param_1', 'test_param_2', key_or_args_indices: [0])
        expect(worker_klass).to have_received(:perform_at).with(
          now + bouncer.delay + bouncer.delay_buffer,
          'test_param_1',
          'test_param_2',
          'WorkerMock:test_param_1'
        )
      end
    end
  end

  describe '#let_in?' do
    context 'when key is nil' do
      it 'does not call Redis' do
        expect(redis_client).not_to have_received(:call)
      end

      it 'returns a timestamp' do
        expect(bouncer.let_in?(nil)).to be_a(Integer)
      end
    end

    context 'when key is not nil' do
      let(:key) { 'WorkerMock:test_param_1,test_param_2' }

      it 'exec call on Redis with GET' do
        bouncer.let_in?(key)

        expect(redis_client)
          .to have_received(:call)
          .with('GET', key)
      end

      context 'when timestamp is in the past' do
        before do
          allow(redis_client).to receive(:call).with('GET', anything).and_return(now - 10)
        end

        it 'returns a timestamp' do
          expect(bouncer.let_in?(key)).to be_a(Integer)
        end
      end

      context 'when timestamp is in the future' do
        before do
          allow(redis_client).to receive(:call).with('GET', anything).and_return(Time.now + 10)
        end

        it 'returns false' do
          expect(bouncer.let_in?(key)).to be(false)
        end
      end

      context 'when debounce timestamp is nil' do
        before do
          allow(redis_client).to receive(:call).with('GET', anything).and_return(nil)
        end

        it 'returns false' do
          expect(bouncer.let_in?(key)).to be(false)
        end
      end
    end
  end

  describe '#run' do
    before do
      # stubbing
      allow(bouncer).to receive(:let_in?).with('do').and_return(1)
      allow(bouncer).to receive(:let_in?).with('do_not').and_return(false)
    end

    context 'when let_in? returns false' do
      it 'returns false' do
        expect(bouncer.run('do_not')).to be(false)
      end

      it 'does not yield' do
        expect { |b| bouncer.run('do_not', &b) }.not_to yield_control
      end

      it 'does not exec call on Redis with DEL' do
        bouncer.run('do_not') { '__test__' }

        expect(redis_client)
          .not_to have_received(:call)
          .with('DEL', anything)
      end
    end

    context 'when let_in? returns 1' do
      it 'returns yield return' do
        expect(bouncer.run('do') { '__test__' }).to be('__test__')
      end

      it 'yields' do
        expect { |b| bouncer.run('do', &b) }.to yield_control
      end

      it 'exec call on Redis with DEL' do
        bouncer.run('do') { '__test__' }

        expect(redis_client)
          .to have_received(:call)
          .with('DEL', 'do')
      end
    end
  end

  describe '#now_i' do
    it 'returns now as integer' do
      expect(bouncer.send(:now_i)).to be(now)
    end
  end

  describe '#redis' do
    it 'returns' do
      expect(bouncer.send(:redis)).to be_a(RedisMock)
    end
  end

  describe '#redis_key' do
    it 'returns now as integer' do
      expect(bouncer.send(:redis_key, 'test_key')).to eql('WorkerMock:test_key')
    end
  end
end

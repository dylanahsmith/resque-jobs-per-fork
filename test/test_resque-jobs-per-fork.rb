require 'test_helper'

class SomeJob
  def self.perform(i)
    $SEQUENCE << "work_#{i}".to_sym
  end
end

class ParentSignalJob
  def self.perform(signal)
    Process.kill(signal, Process.ppid)
  end
end

Resque.before_perform_jobs_per_fork do |worker|
  $SEQUENCE << :before_perform_jobs_per_fork
end

Resque.after_perform_jobs_per_fork do |worker|
  $SEQUENCE << :after_perform_jobs_per_fork
end

class TestResqueMultiJobFork < Test::Unit::TestCase
  def setup
    $SEQUENCE = []

    ENV['JOBS_PER_FORK'] = '2'
    @worker = Resque::Worker.new(:jobs)
    @worker.cant_fork = true
  end

  def test_fewer_jobs_than_per_fork_limit
    Resque::Job.create(:jobs, SomeJob, 1)
    @worker.work(0)

    assert_equal([:before_perform_jobs_per_fork, :work_1, :after_perform_jobs_per_fork], $SEQUENCE)
  end

  def test_same_number_of_jobs_as_per_fork_limit
    Resque::Job.create(:jobs, SomeJob, 1)
    Resque::Job.create(:jobs, SomeJob, 2)
    @worker.work(0)

    assert_equal([:before_perform_jobs_per_fork, :work_1, :work_2, :after_perform_jobs_per_fork], $SEQUENCE)
  end

  def test_more_jobs_than_per_fork_limit
    Resque::Job.create(:jobs, SomeJob, 1)
    Resque::Job.create(:jobs, SomeJob, 2)
    Resque::Job.create(:jobs, SomeJob, 3)
    @worker.work(0)

    assert_equal([
       :before_perform_jobs_per_fork, :work_1, :work_2, :after_perform_jobs_per_fork,
       :before_perform_jobs_per_fork, :work_3, :after_perform_jobs_per_fork
    ], $SEQUENCE)
  end

  def test_should_default_to_one_job_per_fork_if_env_not_set
    ENV.delete('JOBS_PER_FORK')

    assert_nothing_raised(RuntimeError) do
      Resque::Job.create(:jobs, SomeJob, 1)
      Resque::Job.create(:jobs, SomeJob, 2)
      @worker.work(0)

      assert_equal([
         :before_perform_jobs_per_fork, :work_1, :after_perform_jobs_per_fork,
         :before_perform_jobs_per_fork, :work_2, :after_perform_jobs_per_fork
      ], $SEQUENCE)
    end
  end

  if !defined?(RUBY_ENGINE) || defined?(RUBY_ENGINE) && RUBY_ENGINE != "jruby"
    def test_should_stop_running_more_jobs_when_shutting_down
      @worker.cant_fork = false
      Resque::Job.create(:jobs, ParentSignalJob, 'QUIT')
      Resque::Job.create(:jobs, SomeJob, 2)
      @worker.work(0)

      assert next_job = @worker.reserve
      assert_equal SomeJob, next_job.payload_class
    ensure
      @worker.instance_variable_set(:@shutdown, nil)
    end

    def test_should_stop_running_more_jobs_after_pause
      @worker.cant_fork = false
      Resque::Job.create(:jobs, ParentSignalJob, 'USR2')
      Resque::Job.create(:jobs, SomeJob, 2)
      @worker.work(0)

      assert next_job = @worker.reserve
      assert_equal SomeJob, next_job.payload_class
    ensure
      @worker.instance_variable_set(:@pause, nil)
    end
  end
end

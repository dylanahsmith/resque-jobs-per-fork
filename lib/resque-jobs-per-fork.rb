require 'resque'
require 'resque/worker'

module Resque
  # The `before_perform_jobs_per_fork` hooks will run in the child perform
  # right before the child perform starts
  #
  # Call with a block to register a hook.
  # Call with no arguments to return the hooks.
  def before_perform_jobs_per_fork(&block)
    block ? register_hook(:before_perform_jobs_per_fork, block) : hooks(:before_perform_jobs_per_fork)
  end

  # Register `after_perform_jobs_per_fork` hook.
  def before_perform_jobs_per_fork=(before_perform_jobs_per_fork)
    register_hook(:before_perform_jobs_per_fork, before_perform_jobs_per_fork)
  end

  # The `after_perform_jobs_per_fork` hooks will run in the child perform
  # right before the child perform terminates
  #
  # Call with a block to register a hook.
  # Call with no arguments to return the hooks.
  def after_perform_jobs_per_fork(&block)
    block ? register_hook(:after_perform_jobs_per_fork, block) : hooks(:after_perform_jobs_per_fork)
  end

  # Register `after_perform_jobs_per_fork` hook.
  def after_perform_jobs_per_fork=(after_perform_jobs_per_fork)
    register_hook(:after_perform_jobs_per_fork, after_perform_jobs_per_fork)
  end

  class Worker
    def perform_with_jobs_per_fork(job)
      unless @cant_fork
        trap('QUIT') { shutdown }
        trap('USR2') { pause_processing }
      end

      jobs_per_fork = [ENV['JOBS_PER_FORK'].to_i, 1].max

      run_hook :before_perform_jobs_per_fork, self

      jobs_per_fork.times do |attempts|
        break if shutdown? || paused?

        # First attempt's job is passed in from #work in Resque
        if attempts > 0
          # Attempt to reserve another job
          job = reserve

          # Exit early if we run out of work to do
          break unless job

          working_on job
          procline "Processing #{job.queue} since #{Time.now.to_i}"
        end

        perform_without_jobs_per_fork(job)
        processed!
      end

      run_hook :after_perform_jobs_per_fork, self
    end

    alias_method :perform_without_jobs_per_fork, :perform
    alias_method :perform, :perform_with_jobs_per_fork


    def shutdown_with_jobs_per_fork
      shutdown_without_jobs_per_fork
      Process.kill('QUIT', @child) if @child
    end

    alias_method :shutdown_without_jobs_per_fork, :shutdown
    alias_method :shutdown, :shutdown_with_jobs_per_fork

    def pause_processing_with_jobs_per_fork
      pause_processing_without_jobs_per_fork
      Process.kill('USR2', @child) if @child
    end

    alias_method :pause_processing_without_jobs_per_fork, :pause_processing
    alias_method :pause_processing, :pause_processing_with_jobs_per_fork
  end
end

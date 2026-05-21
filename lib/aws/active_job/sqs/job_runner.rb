# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # @api private
      class JobRunner
        attr_reader :id, :class_name

        def initialize(message, queue: nil)
          @queue = queue&.to_sym
          @job_data = job_data(message)
          @class_name = @job_data['job_class'].constantize
          @id = @job_data['job_id']
        end

        def run
          ::ActiveJob::Base.execute(@job_data)
        end

        def exception_executions?
          @job_data['exception_executions'] &&
            !@job_data['exception_executions'].empty?
        end

        private

        def job_data(message)
          if active_job_message?(message)
            ActiveSupport::JSON.load(message.data.body)
          else
            format_event_data(message)
          end
        end

        # Builds the hash passed to ActiveJob::Base.execute for event messages.
        # Keys match the serialized job payload shape (job_class, job_id, arguments).
        def format_event_data(message)
          {
            'job_class' => event_message_class_for(message),
            'job_id' => message.message_id,
            'arguments' => [
              message.data.as_json.merge(
                'receipt_handle' => message.receipt_handle,
                'queue_url' => message.queue_url
              )
            ]
          }
        end

        def active_job_message?(message)
          message.message_attributes.key?('aws_sqs_active_job_class')
        end

        def event_message_class_for(message)
          handler = if @queue
                      Aws::ActiveJob::SQS.config.event_message_class_for(@queue)
                    else
                      Aws::ActiveJob::SQS.config.event_message_class_for_url(message.queue_url)
                    end
          return handler if handler

          target = @queue || message.queue_url
          raise ArgumentError, "No event_message_class configured for queue #{target}"
        end
      end
    end
  end
end

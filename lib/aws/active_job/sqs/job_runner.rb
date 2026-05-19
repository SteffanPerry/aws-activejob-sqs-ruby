# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # @api private
      class JobRunner
        attr_reader :id, :class_name

        def initialize(message)
          @job_data   = prepare_job_data(message)
          @class_name = @job_data['job_class'].constantize
          @id         = @job_data['job_id']
        end

        def run
          ::ActiveJob::Base.execute(@job_data)
        end

        def exception_executions?
          @job_data['exception_executions'] &&
            !@job_data['exception_executions'].empty?
        end

        private

        def prepare_job_data(message)
          return ActiveSupport::JSON.load(message.data.body) if active_job_message?(message)

          format_event_data(message)
        end

        def format_event_data(message)
          {
            'job_class' => job_class_from_config(message.queue_url),
            'job_id' => message.message_id,
            'arguments' => [
              message.data.as_json.merge(
                'receipt_handle' => message.receipt_handle,
                'queue_url' => message.queue_url
              )
            ]
          }
        end

        # Active job messages will have message_attributes key 'aws_sqs_active_job_class'
        def active_job_message?(message)
          !message
            .message_attributes['aws_sqs_active_job_class']
            .nil?
        end

        def job_class_from_config(queue_url)
          handler = Aws::ActiveJob::SQS.config.event_message_class_for_url(queue_url)
          return handler if handler

          raise ArgumentError, "No handler configured for queue #{queue_url}"
        end
      end
    end
  end
end

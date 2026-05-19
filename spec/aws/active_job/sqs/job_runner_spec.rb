# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe JobRunner do
        subject { described_class.new(msg) }

        let(:job_data) { TestJob.new('a1', 'a2').serialize }
        let(:event_body) { 'example sqs message' }
        let(:body) { ActiveSupport::JSON.dump(job_data) }
        let(:message_id) { SecureRandom.uuid }
        let(:active_job_attributes) do
          {
            'aws_sqs_active_job_class' => { 
              'string_value' => 'TestJob',
              'data_type' => 'String'
            },
            'aws_sqs_active_job_version' => {
              'string_value' => '1.0.0',
              'data_type' => 'String'
            }
          }
        end
        let(:msg) do # message is a reserved minitest name
          double( data: double(body: body),
                  message_id: message_id,
                  queue_url: queue_config.dig(:default_queue, :url),
                  message_attributes: active_job_attributes,
                  receipt_handle: SecureRandom.uuid)
        end
        let(:event_msg) do
          double( data: double(message_id: SecureRandom.uuid, body: event_body, message_attributes: {}),
                  queue_url: queue_config.dig(:event_queue, :url),
                  message_id: message_id,
                  message_attributes: {},
                  receipt_handle: SecureRandom.uuid)
        end
        let(:queue_config) do
          {
            default_queue: {
              url: 'http://example.sqs/default_queue',
            },
            event_queue: {
              url: 'http://example.sqs/event_queue',
              event_message_class: 'EventJob'
            }
          }
        end

        before do
          allow(Aws::ActiveJob::SQS.config).to receive(:queues).and_return(queue_config)
        end

        describe '#initialize' do
          describe 'job_data' do
            it 'prepares job_data' do
              expect_any_instance_of(described_class).to receive(:prepare_job_data).with(msg).and_call_original
              subject
            end

            it 'prepares active job data' do
              expect(subject.instance_variable_get(:@job_data)).to eq job_data
            end

            it 'prepares event job data' do
              instance = described_class.new(event_msg, queue: :event_queue)
              expected = described_class.new(event_msg, queue: :event_queue).send(:format_event_data, event_msg)

              expect(instance.instance_variable_get(:@job_data)).to eq(expected)
            end
          end

          it 'sets the class_name' do
            expect(subject.class_name).to eq TestJob
          end

          it 'sets the id' do
            expect(subject.id).to eq job_data['job_id']
          end
        end

        describe '#run' do
          it 'calls Base.execute with the job data' do
            expect(::ActiveJob::Base).to receive(:execute).with(job_data)
            JobRunner.new(msg).run
          end
        end

        describe '#prepare_job_data' do
          before { subject } # initialize the subject

          context 'active job message' do
            it 'returns the job data' do
              expect(ActiveSupport::JSON).to receive(:load).with(body).and_call_original
              expect(subject.send(:prepare_job_data, msg)).to eq job_data
            end
          end

          context 'event message' do
            it 'invokes format_event_data' do
              expect(subject).to receive(:format_event_data).with(event_msg).and_call_original
              subject.send(:prepare_job_data, event_msg)
            end
          end
        end

        describe '#format_event_data' do
          let(:event_job) { described_class.new(event_msg, queue: :event_queue) }

          it 'returns a hash with job_class, job_id, and arguments' do
            message = event_msg.data.as_json.merge(
              'receipt_handle' => event_msg.receipt_handle,
              'queue_url' => event_msg.queue_url
            )

            expect(event_job.send(:format_event_data, event_msg)).to eq(
              'job_class' => 'EventJob',
              'job_id' => event_msg.message_id,
              'arguments' => [message]
            )
          end
        end

        describe '#active_job_message?' do
          it 'returns true if the message has active job attributes' do
            expect(subject.send(:active_job_message?, msg)).to be true
          end

          it 'returns false if the message does not have active job attributes' do
            expect(subject.send(:active_job_message?, event_msg)).to be false
          end
        end

        describe '#event_message_class_for' do
          it 'returns the event_message_class for the queue' do
            event_class = queue_config.dig(:event_queue, :event_message_class)
            runner = described_class.new(event_msg, queue: :event_queue)

            expect(runner.send(:event_message_class_for, event_msg)).to eq(event_class)
          end

          context 'missing event_message_class' do
            it 'raises error' do
              runner = described_class.new(event_msg, queue: :default_queue)
              expect {
                runner.send(:event_message_class_for, event_msg)
              }.to raise_error(ArgumentError, 'No event_message_class configured for queue default_queue')
            end
          end
        end

      end
    end
  end
end

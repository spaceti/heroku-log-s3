require 'aws-sdk-s3'
require 'logger'
require_relative './base.rb'

class Writer < WriterBase

  def list_bucket_objects(s3_client, bucket_name, max_objects = 50)
    if max_objects < 1 || max_objects > 1000
      puts 'Maximum number of objects to request must be between 1 and 1,000.'
      return
    end

    objects = s3_client.list_objects_v2(
      bucket: bucket_name,
      max_keys: max_objects
    ).contents

    if objects.count.zero?
      puts "No objects in bucket '#{bucket_name}'."
      return
    else
      if objects.count == max_objects
        puts "First #{objects.count} objects in bucket '#{bucket_name}':"
      else
        puts "Objects in bucket '#{bucket_name}':"
      end
      objects.each do |object|
        puts object.key
      end
    end
  rescue StandardError => e
    puts "Error accessing bucket '#{bucket_name}' " \
    "or listing its objects: #{e.message}"
  end

  S3_CLIENT = Aws::S3::Client.new({
                                    access_key_id: ENV.fetch('S3_KEY'),
                                    secret_access_key: ENV.fetch('S3_SECRET'),
                                    region: ENV.fetch('AWS_REGION')
                                  })

  # S3_BUCKET_OBJECTS = AWS::S3.new({
  #   access_key_id: ENV.fetch('S3_KEY'),
  #   secret_access_key: ENV.fetch('S3_SECRET'),
  # }).buckets[ENV.fetch('S3_BUCKET')].objects

  def stream_to(filepath)
    @logger.info "begin #{filepath}"
    list_bucket_objects(S3_CLIENT, ENV.fetch('S3_BUCKET'))
    # S3_BUCKET_OBJECTS[filepath].write(
    #   @io,
    #   estimated_content_length: 1 # low-ball estimate; so we can close buffer by returning nil
    # )
    @logger.info "end #{filepath}"
  end

end

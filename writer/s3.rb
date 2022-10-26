require 'aws-sdk-s3'
require 'logger'
require_relative './base.rb'

class Writer < WriterBase

  # Uploads an object to a bucket in Amazon Simple Storage Service (Amazon S3).
  #
  # Prerequisites:
  #
  # - An S3 bucket.
  # - An object to upload to the bucket.
  #
  # @param s3_client [Aws::S3::Client] An initialized S3 client.
  # @param bucket_name [String] The name of the bucket.
  # @param object_key [String] The name of the object.
  # @return [Boolean] true if the object was uploaded; otherwise, false.
  # @example
  #   exit 1 unless object_uploaded?(
  #     Aws::S3::Client.new(region: 'us-east-1'),
  #     'doc-example-bucket',
  #     'my-file.txt'
  #   )
  def object_uploaded?(s3_client, bucket_name, object_key, object_body)
    response = s3_client.put_object(
      bucket: bucket_name,
      key: object_key,
      body: object_body
    )
    if response.etag
      return true
    else
      return false
    end
  rescue StandardError => e
    puts "Error uploading object: #{e.message}"
    return false
  end

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

  def stream_to(filepath)
    @logger.info "begin #{filepath}"
    list_bucket_objects(S3_CLIENT, ENV.fetch('S3_BUCKET'))
    # S3_BUCKET_OBJECTS[filepath].write(
    #   @io,
    #   estimated_content_length: 1 # low-ball estimate; so we can close buffer by returning nil
    # )
    if object_uploaded?(S3_CLIENT, ENV.fetch('S3_BUCKET'), filepath, @io.read())
      puts "Object '#{filepath}' uploaded to bucket '#{ENV.fetch('S3_BUCKET')}'."
    else
      puts "Object '#{filepath}' not uploaded to bucket '#{ENV.fetch('S3_BUCKET')}'."
    end
    @logger.info "end #{filepath}"
  end

end

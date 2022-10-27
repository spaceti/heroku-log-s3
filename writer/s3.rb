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

  S3_CLIENT = Aws::S3::Client.new({
                                    access_key_id: ENV.fetch('S3_KEY'),
                                    secret_access_key: ENV.fetch('S3_SECRET'),
                                    region: ENV.fetch('AWS_REGION')
                                  })

  def stream_to(filepath)
    @logger.info "begin #{filepath}"
    # S3_BUCKET_OBJECTS[filepath].write(
    #   @io,
    #   estimated_content_length: 1 # low-ball estimate; so we can close buffer by returning nil
    # )
    all_data = []
    while data = @io.read(4068)
      all_data.push data
    end

    @logger.info all_data

    # if object_uploaded?(S3_CLIENT, ENV.fetch('S3_BUCKET'), filepath, all_data)
    #   puts "Object '#{filepath}' uploaded to bucket '#{ENV.fetch('S3_BUCKET')}'."
    # else
    #   puts "Object '#{filepath}' not uploaded to bucket '#{ENV.fetch('S3_BUCKET')}'."
    # end
    @logger.info "end #{filepath}"
  end

end

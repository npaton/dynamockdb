Bundler.require

# AWS.config(dynamo_db_endpoint:"localhost", dynamo_db_port:"8080", access_key_id: ENV["AWS_ACCESS_KEY_ID"], secret_access_key: ENV["AWS_SECRET_ACCESS_KEY"])
AWS.config(dynamo_db_proxy_uri: 'http://localhost:3000', dynamo_db_use_ssl: false, access_key_id: ENV["AWS_ACCESS_KEY_ID"], secret_access_key: ENV["AWS_SECRET_ACCESS_KEY"])
AWS.config(access_key_id: ENV["AWS_ACCESS_KEY_ID"], secret_access_key: ENV["AWS_SECRET_ACCESS_KEY"])


dynamo_db = AWS::DynamoDB.new

dynamo_db.tables.each do |table|
	puts table.name
end
dynamo_db.tables['fh-data-import-tasks-data'].status


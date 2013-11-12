require "curb"

module NoDB

	class Orchestrate

		# ===================================================================================
		#  Initialize, plus a few private routines to perform basic HTTP actions
		
		def initialize(orch_config={})
			@base_url = orch_config['base-url']  # https://api.orchestrate.io/v0
			@user     = orch_config[:user]      # <user-key-from-orchestrate.io>
			@verbose  = orch_config[:verbose] if orch_config[:verbose]
		end

		# -------------------------------------------------------------------
		#  Use Curl to do the HTTP stuff (GET, PUT, DELETE)
		# -------------------------------------------------------------------
		def do_the_get_call(args)
			puts "----- GET: #{args[:url]}" if @verbose
			c = Curl::Easy.new(args[:url]) do |curl|
				curl.http_auth_types = :basic
				curl.username = @user
			end
			c.perform
			c.body_str
		end

		def do_the_put_call(args)
			puts "----- PUT: #{args[:url]}" if @verbose
			c = Curl::Easy.http_put(args[:url], args[:json]) do |curl|
				curl.headers['Accept'] = 'application/json'
				curl.headers['Content-Type'] = 'application/json'
				curl.http_auth_types = :basic
				curl.username = @user
			end
			c.perform
		  # c.body_str
		end

		def do_the_delete_call(args)
			puts "----- DELETE: #{args[:url]}" if @verbose
			c = Curl::Easy.new(args[:url]) do |curl|
				curl.http_auth_types = :basic
				curl.username = @user
				curl.delete = true
			end
			c.perform
			c.inspect
		end

		private :do_the_get_call, :do_the_put_call, :do_the_delete_call

		# ===================================================================================
		#  Public routines to perform basic Orchestrate actions

		# -------------------------------------------------------------------
		#  Name: query
		#  Desc: query the collection
		#  Args: collection, query
		# -------------------------------------------------------------------
		def query(args)
			api_url = "#{@base_url}/#{args[:collection]}/?query=#{args[:query]}"
			do_the_get_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: get_collection
		#  Desc: get the entire collection
		#  Args: collection
		# -------------------------------------------------------------------
		def get_collection(args)
			api_url = "#{@base_url}/#{args[:collection]}"
			do_the_get_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: put_collection
		#  Desc: create a new collection
		#  Args: collection
		# -------------------------------------------------------------------
		def put_collection(args)
			api_url = "#{@base_url}/#{args[:collection]}"
			do_the_put_call( url: api_url, json: '{}' )
		end

		# -------------------------------------------------------------------
		#  Name: delete_collection
		#  Desc: delete the entire collection
		#  Args: collection
		# -------------------------------------------------------------------
		def delete_collection(args)
			api_url = "#{@base_url}/#{args[:collection]}"
			do_the_delete_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: get_key
		#  Desc: return specified 'row' of data (key)
		#  Args: collection, key
		# -------------------------------------------------------------------
		def get_key(args)
			api_url = "#{@base_url}/#{args[:collection]}/#{args[:key]}"
			do_the_get_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: put_key
		#  Desc: add specified 'row' of data (key)
		#  Args: collection, key, json (string)
		# -------------------------------------------------------------------
		def put_key(args)
			api_url = "#{@base_url}/#{args[:collection]}/#{args[:key]}"
			do_the_put_call( url: api_url, json: args[:json] )
		end

		# -------------------------------------------------------------------
		#  Name: delete_key
		#  Desc: delete specified 'row' of data (key)
		#  Args: collection, key
		# -------------------------------------------------------------------
		def delete_key(args)
			api_url = "#{@base_url}/#{args[:collection]}/#{args[:key]}"
			do_the_delete_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: get_events
		#  Desc: return requested events from specified key
		#  Args: collection, key, event_type
		# -------------------------------------------------------------------
		def get_events(args)
			api_url = "#{@base_url}/#{args[:collection]}/#{args[:key]}/events/#{args[:event_type]}"
			do_the_get_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: put_event
		#  Desc: add event to specified key
		#  Args: collection, key, event_type, json (string)
		# -------------------------------------------------------------------
		def put_event(args)
			api_url = "#{@base_url}/#{args[:collection]}/#{args[:key]}/events/#{args[:event_type]}"
			do_the_put_call( url: api_url, json: args[:json] )
		end

		# -------------------------------------------------------------------
		#  Name: delete_events
		#  Desc: return requested events from specified key
		#  Args: collection, key, event_type
		# -------------------------------------------------------------------
		def delete_events(args)
			api_url = "#{@base_url}/#{args[:collection]}/#{args[:key]}/events/#{args[:event_type]}"
			do_the_delete_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: get_relations
		#  Desc: return requested relations from specified key
		#  Args: collection, key, relation
		# -------------------------------------------------------------------
		def get_relations(args)
			api_url = "#{@base_url}/#{args[:collection]}/#{args[:key]}/relations/#{args[:relation]}"
			do_the_get_call( url: api_url )
		end

		# # -------------------------------------------------------------------
		# #  Name: get_relation
		# #  Desc: return requested relation from specified key
		# #  Args: collection, key, relation
		# # -------------------------------------------------------------------
		# def get_relation(args)
		# 	api_url = "#{@base_url}/#{args[:collection_A]}/#{args[:key_A]}/relations/#{args[:relation]}" +
		# 												 "#{args[:collection_B]}/#{args[:key_B]}"
		# 	do_the_get_call( url: api_url )
		# end

		# -------------------------------------------------------------------
		#  Name: put_relation
		#  Desc: create requested relation for specified key (coll_A/key_A)
		#  Args: collection_A, key_A, relation, collection_B, key_B
		# -------------------------------------------------------------------
		def put_relation(args)
			api_url = "#{@base_url}/#{args[:collection_A]}/#{args[:key_A]}/relation/" +
			                       "#{args[:relation]}/#{args[:collection_B]}/#{args[:key_B]}"
			do_the_put_call( url: api_url, json: '{}')
		end

		# -------------------------------------------------------------------
		#  Name: delete_relations
		#  Desc: delete specified relations (all) from specified key
		#  Args: collection, key, relation
		# -------------------------------------------------------------------
		def delete_relations(args)
			api_url = "#{@base_url}/#{args[:collection_A]}/#{args[:key_A]}/relation/" +
			                       "#{args[:relation]}"
			do_the_delete_call( url: api_url )
		end

		# -------------------------------------------------------------------
		#  Name: delete_relation
		#  Desc: delete specified relations from specified key
		#  Args: collection, key, relation
		# -------------------------------------------------------------------
		def delete_relation(args)
			api_url = "#{@base_url}/#{args[:collection_A]}/#{args[:key_A]}/relation/" +
			                       "#{args[:relation]}/#{args[:collection_B]}/#{args[:key_B]}"
			do_the_delete_call( url: api_url )
		end
		
	end # class Orchestrate

end # module NoDB


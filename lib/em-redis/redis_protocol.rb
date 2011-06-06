require 'rubygems'
require 'eventmachine'

module EventMachine
  module Protocols
    module Redis
      include EM::Deferrable

      ##
      # constants
      #########################

      OK      = "OK".freeze
      MINUS    = "-".freeze
      PLUS     = "+".freeze
      COLON    = ":".freeze
      DOLLAR   = "$".freeze
      ASTERISK = "*".freeze
      DELIM    = "\r\n".freeze

      BOOLEAN_PROCESSOR = lambda{|r| r == 1 }

      REPLY_PROCESSOR = {
        "exists"    => BOOLEAN_PROCESSOR,
        "sismember" => BOOLEAN_PROCESSOR,
        "sadd"      => BOOLEAN_PROCESSOR,
        "srem"      => BOOLEAN_PROCESSOR,
        "smove"     => BOOLEAN_PROCESSOR,
        "zadd"      => BOOLEAN_PROCESSOR,
        "zrem"      => BOOLEAN_PROCESSOR,
        "move"      => BOOLEAN_PROCESSOR,
        "setnx"     => BOOLEAN_PROCESSOR,
        "del"       => BOOLEAN_PROCESSOR,
        "renamenx"  => BOOLEAN_PROCESSOR,
        "expire"    => BOOLEAN_PROCESSOR,
        "select"    => BOOLEAN_PROCESSOR, # not in redis gem
        "hset"      => BOOLEAN_PROCESSOR,
        "hdel"      => BOOLEAN_PROCESSOR,
        "hexists"   => BOOLEAN_PROCESSOR,
        "keys"      => lambda {|r|
          if r.is_a?(Array)
            r
          else
            r.split(" ")
          end
        },
        "info"      => lambda{|r|
          info = {}
          r.each_line {|kv|
            k,v = kv.split(":",2).map{|x| x.chomp}
            info[k.to_sym] = v
          }
          info
        }, 
        "hgetall"   => lambda{|r|
          Hash[*r]
        }
      }

      ALIASES = {
        "flush_db"             => "flushdb",
        "flush_all"            => "flushall",
        "last_save"            => "lastsave",
        "key?"                 => "exists",
        "delete"               => "del",
        "randkey"              => "randomkey",
        "list_length"          => "llen",
        "push_tail"            => "rpush",
        "push_head"            => "lpush",
        "pop_tail"             => "rpop",
        "pop_head"             => "lpop",
        "list_set"             => "lset",
        "list_range"           => "lrange",
        "list_trim"            => "ltrim",
        "list_index"           => "lindex",
        "list_rm"              => "lrem",
        "set_add"              => "sadd",
        "set_delete"           => "srem",
        "set_count"            => "scard",
        "set_member?"          => "sismember",
        "set_members"          => "smembers",
        "set_intersect"        => "sinter",
        "set_intersect_store"  => "sinterstore",
        "set_inter_store"      => "sinterstore",
        "set_union"            => "sunion",
        "set_union_store"      => "sunionstore",
        "set_diff"             => "sdiff",
        "set_diff_store"       => "sdiffstore",
        "set_move"             => "smove",
        "set_unless_exists"    => "setnx",
        "rename_unless_exists" => "renamenx",
        "type?"                => "type",
        "zset_add"             => "zadd",
        "zset_count"           => "zcard",
        "zset_range_by_score"  => "zrangebyscore",
        "zset_reverse_range"   => "zrevrange",
        "zset_range"           => "zrange",
        "zset_delete"          => "zrem",
        "zset_score"           => "zscore",
        "zset_incr_by"         => "zincrby",
        "zset_increment_by"    => "zincrby",
        # these aliases aren't in redis gem
        "background_save"      => 'bgsave',
        "async_save"           => 'bgsave',
        "members"              => 'smembers',
        "decrement_by"         => "decrby",
        "decrement"            => "decr",
        "increment_by"         => "incrby",
        "increment"            => "incr",
        "set_if_nil"           => "setnx",
        "multi_get"            => "mget",
        "random_key"           => "randomkey",
        "random"               => "randomkey",
        "rename_if_nil"        => "renamenx",
        "tail_pop"             => "rpop",
        "pop"                  => "rpop",
        "head_pop"             => "lpop",
        "shift"                => "lpop",
        "list_remove"          => "lrem",
        "index"                => "lindex",
        "trim"                 => "ltrim",
        "list_range"           => "lrange",
        "range"                => "lrange",
        "list_len"             => "llen",
        "len"                  => "llen",
        "head_push"            => "lpush",
        "unshift"              => "lpush",
        "tail_push"            => "rpush",
        "push"                 => "rpush",
        "add"                  => "sadd",
        "set_remove"           => "srem",
        "set_size"             => "scard",
        "member?"              => "sismember",
        "intersect"            => "sinter",
        "intersect_and_store"  => "sinterstore",
        "members"              => "smembers",
        "exists?"              => "exists"
      }

      DISABLED_COMMANDS = {
        "monitor" => true,
        "sync"    => true
      }

      def []=(key,value)
        set(key,value)
      end

      def set(key, value, expiry=nil)
        call_command([:set, key, value]) do |s|
          yield s if block_given?
        end
        expire(key, expiry) if expiry
      end

      def sort(key, options={}, &blk)
        cmd = ["SORT"]
        cmd << key
        cmd << ["BY", options[:by]] if options[:by]
        cmd << [options[:get]].flatten.map { |key| ["GET", key] } if options[:get]
        cmd << options[:order].split(/\s+/) if options[:order]
        cmd << ["LIMIT", options[:limit]] if options[:limit]
        call_command(cmd.flatten, &blk)
      end

      def incr(key, increment = nil, &blk)
        call_command(increment ? ["incrby",key,increment] : ["incr",key], &blk)
      end

      def decr(key, decrement = nil, &blk)
        call_command(decrement ? ["decrby",key,decrement] : ["decr",key], &blk)
      end

      def select(db, &blk)
        @db = db.to_i
        call_command(['select', @db], &blk)
      end

      def auth(password, &blk)
        @password = password
        call_command(['auth', password], &blk)
      end

      # Similar to memcache.rb's #get_multi, returns a hash mapping
      # keys to values.
      def mapped_mget(*keys)
        mget(*keys) do |response|
          result = {}
          response.each do |value|
            key = keys.shift
            result.merge!(key => value) unless value.nil?
          end
          yield result if block_given?
        end
      end

      # Ruby defines a now deprecated type method so we need to override it here
      # since it will never hit method_missing
      def type(key, &blk)
        call_command(['type', key], &blk)
      end

      def quit(&blk)
        call_command(['quit'], &blk)
      end

      def exec(&blk)
        call_command(['exec'], &blk)
      end

      # I'm not sure autocommit is a good idea.
      # For example:
      #   r.multi { r.set('a', 'b') { raise "kaboom" } }
      # will commit "a" and will stop EM
      def multi
        call_command(['multi'])
        if block_given?
          begin
            yield self
            exec
          rescue Exception => e
            discard
            raise e
          end
        end
      end

      def mset(*args, &blk)
        hsh = args.pop if Hash === args.last
        if hsh
          call_command(hsh.to_a.flatten.unshift(:mset), &blk)
        else
          call_command(args.unshift(:mset), &blk)
        end
      end

      def msetnx(*args, &blk)
        hsh = args.pop if Hash === args.last
        if hsh
          call_command(hsh.to_a.flatten.unshift(:msetnx), &blk)
        else
          call_command(args.unshift(:msetnx), &blk)
        end
      end

      def subscribe(channel, on_msg_callback, &blk)
        @subscribed = true
        @redis_subscriptions[channel] = on_msg_callback
        call_command(['subscribe', channel], &blk)
      end

      def psubscribe(channel, on_msg_callback, &blk)
        @subscribed = true
        @redis_psubscriptions[channel] = on_msg_callback
        call_command(['psubscribe', channel], &blk)
      end

      def errback(&blk)
        @error_callback = blk
      end
      alias_method :on_error, :errback

      def method_missing(*argv, &blk)
        call_command(argv, &blk)
      end

      def maybe_lock(&blk)
        if !EM.reactor_thread?
          EM.schedule { maybe_lock(&blk) }
        elsif @connected
          yield
        else
          callback { yield }
        end
      end

      def call_command(argv, &blk)
        argv = argv.dup

        argv[0] = argv[0].to_s.downcase
        argv[0] = ALIASES[argv[0]] if ALIASES[argv[0]]
        raise "#{argv[0]} command is disabled" if DISABLED_COMMANDS[argv[0]]

        command = ""
        command << "*#{argv.size}\r\n"
        argv.each do |a|
          a = a.to_s
          command << "$#{get_size(a)}\r\n"
          command << a
          command << "\r\n"
        end

        @logger.debug { "*** sending: #{command}" } if @logger
        maybe_lock do
          @redis_callbacks << [REPLY_PROCESSOR[argv[0]], blk]
          send_data command
        end
      end

      ##
      # errors
      #########################

      class ParserError < StandardError; end
      class ProtocolError < StandardError; end

      class RedisError < StandardError
        attr_accessor :code
      end


      ##
      # em hooks
      #########################

      def self.connect(*args)
        case args.length
        when 0
          options = {}
        when 1
          arg = args.shift
          case arg
          when Hash then options = arg
          when String then options = {:host => arg}
          else raise ArgumentError, 'first argument must be Hash or String'
          end
        when 2
          options = {:host => args[0], :port => args[1]}
        else
          raise ArgumentError, "wrong number of arguments (#{args.length} for 1)"
        end
        options[:host] ||= '127.0.0.1'
        options[:port]   = (options[:port] || 6379).to_i
        EM.connect options[:host], options[:port], self, options
      end

      def initialize(options = {})
        @host           = options[:host]
        @port           = options[:port]
        @db             = (options[:db] || 0).to_i
        @password       = options[:password]
        @logger         = options[:logger]
        @error_callback = lambda do |code|
          err = RedisError.new
          err.code = code
          raise err, "Redis server returned error code: #{code}"
        end
        @redis_subscriptions = {}
        @redis_psubscriptions = {}

        # These commands should be first
        auth_and_select_db
      end

      def auth_and_select_db
        call_command(["auth", @password]) if @password
        call_command(["select", @db]) unless @db == 0
      end
      private :auth_and_select_db

      def connection_completed
        @logger.debug { "Connected to #{@host}:#{@port}" } if @logger

        @redis_callbacks = []
        @previous_multibulks = []
        @multibulk_n     = false
        @reconnecting    = false
        @connected       = true

        succeed
      end

      # 19Feb09 Switched to a custom parser, LineText2 is recursive and can cause
      #         stack overflows when there is too much data.
      # include EM::P::LineText2
      def receive_data(data)
        (@buffer ||= '') << data
        while index = @buffer.index(DELIM)
          begin
            line = @buffer.slice!(0, index+2)
            process_cmd line
          rescue ParserError
            @buffer[0...0] = line
            break
          end
        end
      end

      def process_cmd(line)
        @logger.debug { "*** processing #{line}" } if @logger
        # first character of buffer will always be the response type
        reply_type = line[0, 1]
        reply_args = line.slice(1..-3) # remove type character and \r\n
        case reply_type

        #e.g. -MISSING
        when MINUS
          @redis_callbacks.shift # throw away the cb?
          @error_callback.call(reply_args)
        # e.g. +OK
        when PLUS
          dispatch_response(reply_args)
        # e.g. $3\r\nabc\r\n
        # 'bulk' is more complex because it could be part of multi-bulk
        when DOLLAR
          data_len = Integer(reply_args)
          if data_len == -1 # expect no data; return nil
            dispatch_response(nil)
          elsif @buffer.size >= data_len + 2 # buffer is full of expected data
            dispatch_response(@buffer.slice!(0, data_len))
            @buffer.slice!(0,2) # tossing \r\n
          else # buffer isn't full or nil
            # TODO: don't control execution with exceptions
            raise ParserError
          end
        #e.g. :8
        when COLON
          dispatch_response(Integer(reply_args))
        #e.g. *2\r\n$1\r\na\r\n$1\r\nb\r\n 
        when ASTERISK
          multibulk_count = Integer(reply_args)
          if multibulk_count == -1 || multibulk_count == 0
            dispatch_response([])
          else
            if @multibulk_n
              @previous_multibulks << [@multibulk_n, @multibulk_values]
            end
            @multibulk_n = multibulk_count
            @multibulk_values = []
          end
        # Whu?
        else
          # TODO: get rid of this exception
          raise ProtocolError, "reply type not recognized: #{line.strip}"
        end
      end

      def dispatch_response(value)
        if @multibulk_n
          @multibulk_values << value
          @multibulk_n -= 1

          if @multibulk_n == 0
            value = @multibulk_values
            @multibulk_n,@multibulk_values = @previous_multibulks.pop
            if @multibulk_n
              dispatch_response(value)
              return
            end
          else
            return
          end
        end

        if @subscribed and value
          case value[0]
          when "message", "pmessage"
            return process_message(value)
          when "unsubscribe", "punsubscribe"
            proccess_unsubscribe(value)
          end
        end

        processor, blk = @redis_callbacks.shift
        value = processor.call(value) if processor
        blk.call(value) if blk
      end

      def process_message(value)
        type = value.shift
        if type == "message"
          channel = value.shift
          message = value.shift
          @redis_subscriptions[channel].call(message)
        else
          pattern = value.shift
          channel = value.shift
          message = value.shift
          @redis_psubscriptions[pattern].call(channel, message)
        end
      end

      def proccess_unsubscribe(value)
        type = value.shift
        channel = value.shift
        if type == "unsubscribe"
          @redis_subscriptions.delete(channel)
        else
          @redis_psubscriptions.delete(channel)
        end
        @subscribed = @redis_subscriptions.size + @redis_subscriptions.size > 0
      end

      def unbind
        @logger.debug { "Disconnected" }  if @logger
        if @connected || @reconnecting
          EM.add_timer(1) do
            @logger.debug { "Reconnecting to #{@host}:#{@port}" }  if @logger
            reconnect @host, @port
            auth_and_select_db
          end
          @connected = false
          @reconnecting = true
          @deferred_status = nil
        else
          # TODO: get rid of this exception
          raise 'Unable to connect to redis server'
        end
      end

      private
        def get_size(string)
          string.respond_to?(:bytesize) ? string.bytesize : string.size
        end

    end
  end
end

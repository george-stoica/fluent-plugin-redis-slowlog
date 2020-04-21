require 'fluent/input'

class Fluent::Redis_SlowlogInput < Fluent::Input
  Fluent::Plugin.register_input('redis_slowlog', self)

  config_param :tag,      :string
  config_param :host,     :string,  :default => nil
  config_param :port,     :integer, :default => 6379
  config_param :password, :string,  :default => nil
  config_param :logsize,  :integer,  :default => 128
  config_param :interval, :integer,  :default => 10


  def initialize
    super
    require 'redis'
  end

  def configure(conf)
    super
    @log_id = -1
    @get_interval = @interval
  end

  def start
    super
    @redis = Redis.new(
      :host => @host,
      :port => @port,
      :password => @password,
      :thread_safe => true
    )
    @watcher = Thread.new(&method(:watch))
  end

  def shutdown
    super
    @redis.quit
  end

  private
  def watch
    while true
      sleep @get_interval
      begin
        pong = @redis.ping
        if pong == 'PONG'
          @log_id = output(@log_id)
        else
          log.error "fluent-plugin-redis-slowlog: ping failed"
        end
      rescue StandardError => e
        log.error e.message
      end
    end
  end

  unless method_defined?(:router)
    define_method("router") { Fluent::Engine }
  end

  def output(last_id = -1)
    slow_logs = []
    slow_logs = @redis.slowlog('get', logsize)

    if slow_logs.empty?
      return -1
    end

    log_id = slow_logs[0][0]
    slow_logs.reverse.each do |log|
      unless log[0] > last_id
        next
      end

      log_hash = { 
        level: 'info',
        time: Time.at(log[1]).strftime("%Y-%m-%dT%H:%M:%S.%LZ"),
        message: "redis_host: #{@host}, redis_port: #{@port}, slowlog_id: #{log[0]}, exec_time: #{log[2]}, command: #{log[3]}",
        context: { footprint: "" }
      }
      
      router.emit(tag, Time.now.to_i, log_hash)
    end
    return log_id
  end
end

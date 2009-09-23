require 'socket'
require 'thread'

class Proxy
  attr_reader :index, :source, :dest
  attr_accessor :source_ready

  class DestError < StandardError
  end
  
  def set_options(sock)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
    sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)    
  end
  
  def initialize(source, delegate, index)
    @source, @delegate, @index =
      source, delegate, index
    @shutting_down = false
    @dest = nil
    @start = nil
    @source_ready = @terminated = false

    @mutex = Mutex.new

    set_options(@source)
  end

  TERMINATOR = '~|~|~|~|~|~5818df499987fab124fac0cfc9edb83fa5578c36'
  TERM_LENGTH = TERMINATOR.length
  TERM_RE = Regexp.new(Regexp.quote(TERMINATOR))

  def pull_thread
    begin
      stopped = false
      data = nil
      while !stopped and !@shutting_down and IO.select([@source])
        begin
          block = @source.read_nonblock(1440)
          # puts "Pull received: #{block.length}"
          unless block and !block.empty?
            # puts "#{@index}: read returned no data"
            break
          end
          
          data ||= ''
          data << block
          next if data.length < TERM_LENGTH
          
          if data =~ TERM_RE
            # puts "#{@index}: Received terminator... stopping"
            stopped = true
            data = $`
          else
            next if data.index(?~)
          end

          @dest.write(data) if @dest
          data = nil
          
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "EAGAIN"
        end
      end
    
    rescue Errno::EPIPE, Errno::ECONNRESET, EOFError
      puts $!, $!.class
      # puts "#{@index}: Pull: socket closed"

    rescue
      puts $!, $!.class
      puts $!.backtrace.join("\n")
    end

    # puts "#{@index}: Pull loop terminated"
    
    shutdown if !stopped
    shutdown_dest

  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end

  def push_thread
    begin
      while @dest and !@shutting_down and IO.select([@dest])
        begin
          data = nil
          # puts "#{@index}: push_thread Waiting for mutex"
          @mutex.synchronize do
            break unless @dest
            # puts "#{@index}: Reading data..."
            data = @dest.read_nonblock(1440)
            # puts "#{@index}: Read data..."
          end
          
          unless data and !data.empty?
            # puts "#{@index}: read returned no data"
            break
          end
          
          #puts "#{@index}: Push received: #{data.length}"
          
          @source.write(data)
          
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "#{@index}: EAGAIN"
        end
      end
      
    rescue Errno::EPIPE, EOFError, Errno::ECONNRESET
      # puts "#{@index}: Push: Socket closed"
    rescue
      puts $!, $!.class
      puts $!.backtrace.join("\n")
    end

    # puts "#{@index}: Push terminated"
    
    shutdown_dest
    
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end

  def create_threads
    @threads = [Thread.new { push_thread },
                Thread.new { pull_thread }]
  end

  def kill_threads
    if @threads
      threads, @threads = @threads, nil
      threads.each do |t|
        t.kill unless Thread.current == t
      end
    end
  end

  def send_terminator(force = false)
    # puts "#{@index}: send_terminator: Waiting for mutex"
    @mutex.synchronize do
      return if @terminated and !force
      @terminated = true
    end
    
    # puts "#{@index}: Writing terminator..."
    @source.write(TERMINATOR)
    @source.flush
            
  rescue Errno::ESHUTDOWN
    puts "Socket already closed"
  end

  def dest=(dest)
    @mutex.synchronize do
      if @dest
        raise DestError, "#{@index}: Can't set dest when one is already present"
      end
      # puts "#{@index}: Setting dest to: #{dest}"
      @dest = dest
    end

    @source_ready = false
    @terminated = false

    set_options(@dest)
    create_threads
  end
  
  def start
    @start = Time.now
  end
  
  def shutdown
    puts "#{@index}: #{@start - Time.now} seconds" if @start
    
    # puts "#{@index}: shutdown waiting for mutex"
    @mutex.synchronize do 
      return if @shutting_down
      @shutting_down = true
    end

    # puts "#{@index}: Shutting down proxy for #{self.index}"
    @source.shutdown rescue
    if @dest
      begin
        @dest.shutdown
      rescue
      end
    end
    
  rescue
    puts $!
    puts $!.backtrace.join("\n")
  end

  def flush_source
    # puts "#{@index}: Flushing source..."
    while data = @source.read_nonblock(1440)
      puts "Flushed: #{data.inspect}" if data != TERMINATOR
    end
  rescue Errno::EAGAIN, Errno::EWOULDBLOCK, EOFError
  end

  def shutdown_dest
    puts "#{@index}: #{@start - Time.now} seconds" if @start
    
    dest = nil
    # puts "#{@index}: shutdown_dest waiting for mutex"
    @mutex.synchronize do 
      return if @dest.nil? or @shutting_down
      # puts "#{@index}: Shutting down destination #{self.index}, source still connected"
      dest, @dest = @dest, nil
    end


    begin
      send_terminator
      dest.shutdown
    rescue Errno::ENOTCONN, Errno::ESHUTDOWN
      # Ignore
    rescue
      puts "#{@index}: shutting down dest: #{$!}", $!.class
    end
    
    flush_source
    
    # puts "#{@index}: Shutdown dest calling delegate for #{self.index}"
    @delegate.shutdown_remote(self)
    
  rescue Errno::ENOTCONN, Errno::ESHUTDOWN
    # puts "Ignoring not connected"
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end
end

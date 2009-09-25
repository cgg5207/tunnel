require 'socket'
require 'thread'

class Proxy
  attr_reader :index, :source, :dest, :active
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
    @source_ready = @terminated = false
    @start = nil
    @close_count = 0

    @mutex = Mutex.new

    set_options(@source)
  end

  TERMINATOR = 0.chr * 5 + '1818df499987fab124fac0cfc9edb83fa5578c36'
  TERM_LENGTH = TERMINATOR.length
  TERM_RE = Regexp.new(Regexp.quote(TERMINATOR))

  def pull_thread
    begin
      close_read = false 
      stopped = false
      data = nil
      while !stopped and !@shutting_down and IO.select([@source])
        begin
          block = @source.read_nonblock(1440)
          puts "Pull received: #{block.length}" if VERBOSE
          unless block and !block.empty?
            puts "#{@index}: read returned no data"
            break
          end
          
          data ||= ''
          data << block
          next if data.length < TERM_LENGTH
          
          if data =~ TERM_RE
            puts "#{@index}: Received terminator... stopping"  if VERBOSE
            stopped = true
            data = $`
          elsif data.index(0.chr)
            puts "Found \\0"  if VERBOSE
            next
          end
          
          if @dest and data.length > 0
            puts "#{@index}: pull Writing #{data.length} bytes"  if VERBOSE
            @dest.write(data) 
            puts "#{@index}: pull Wrote..."  if VERBOSE
            data = nil
          end
          
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "#{@index}: EAGAIN (pull)"
        end
      end
    
    rescue Errno::EPIPE, Errno::ECONNRESET
      puts $!, $!.class
      puts "#{@index}: Pull: socket closed"
      puts $!.backtrace[0]
      
    rescue EOFError
      puts $!, $!.class
      puts "#{@index}: Pull: socket closed"
      puts $!.backtrace[0]
      
    rescue
      puts $!, $!.class
      puts $!.backtrace.join("\n")
    end
    
    puts "#{@index}: Pull loop terminated" if VERBOSE
    
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")

  ensure
    shutdown if !stopped
    shutdown_write
  end

  def push_thread
    begin
      while @dest and !@shutting_down and IO.select([@dest])
        begin
          data = nil
          @mutex.synchronize do
            break unless @dest
            data = @dest.read_nonblock(1440)
          end
          
          unless data and !data.empty?
            puts "#{@index}: read returned no data" if VERBOSE
            break
          end
          
          puts "#{@index}: Push received: #{data.length}" if VERBOSE
          @source.write(data)
          
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "#{@index}: EAGAIN (push)" if VERBOSE
        end
      end
      
    rescue Errno::EPIPE, EOFError, Errno::ECONNRESET, Errno::ENOTSOCK
      puts "#{@index}: Push: Socket closed\n#{$!.backtrace[0]}\n#{$!}\n2#{$!.class}" if VERBOSE
      
    rescue
      puts $!, $!.class
      puts $!.backtrace.join("\n")
    end
    
    puts "#{@index}: Push terminated" if VERBOSE
    
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
    
  ensure
    shutdown_read
  end
  
  def create_threads
    @threads = [Thread.new { push_thread },
                Thread.new { pull_thread }]
  end

  def kill_threads
    if @threads
      @threads.each do |t|
        puts "#{@index}: Killing thread #{t.inspect}" if VERBOSE
        t.kill unless Thread.current == t
      end
    end
  end

  def send_terminator(force = false)
    puts "#{@index}: send_terminator: Waiting for mutex" if VERBOSE
    @mutex.synchronize do
      return if @terminated and !force
      @terminated = true
    end
    
    puts "#{@index}: Writing terminator..." if VERBOSE
    @source.write(TERMINATOR)
    @source.flush
            
  rescue Errno::ESHUTDOWN
    puts "Socket already closed"
  end

  def dest=(dest)
    @mutex.synchronize do
      if @dest
        raise DestError, "#{@index}: Can't set dest when one is already present" if VERBOSE
      end
      # puts "#{@index}: Setting dest to: #{dest}"
      @dest = dest
    end

    @source_ready = false
    @terminated = false
    @close_count = 0

    set_options(@dest)
    create_threads
  end
  
  def start
    @start = Time.now
  end

  def shutdown
    puts "#{@index}: shutdown waiting for mutex" if VERBOSE
    @mutex.synchronize do 
      return if @shutting_down
      @shutting_down = true
    end

    puts "#{@index}: Shutting down proxy for #{self.index}" if VERBOSE
    @source.close rescue
    if @dest
      begin
        @dest.close
      rescue
      end
    end
    
  rescue
    puts $!
    puts $!.backtrace.join("\n")
  end

  def flush_source
    puts "#{@index}: Flushing source..." if VERBOSE
    while data = @source.read_nonblock(1440)
      puts "Flushed: #{data.inspect}" if data != TERMINATOR
    end
  rescue Errno::EAGAIN, Errno::EWOULDBLOCK, EOFError
  end

  def increment_close_count
    count = nil
    @mutex.synchronize do 
      count = (@close_count += 1)
    end
    count
  end

  def shutdown_read
    puts "#{@index}: shutting down read side of proxy" if VERBOSE

    count = dest = nil
    @mutex.synchronize do 
      return if @dest.nil? or @shutting_down
      puts "#{@index}: Shutting down read" if VERBOSE
      dest = @dest
      count = (@close_count += 1)
    end

    begin
      dest.close_read
    rescue Errno::ENOTCONN, Errno::ESHUTDOWN
      puts "#{@index}: Close read: #{$!}" if VERBOSE
    end
    
    send_terminator

  rescue Errno::ENOTCONN, Errno::ESHUTDOWN
    # Ignore
  rescue
    puts "#{@index}: shutting down dest: #{$!}", $!.class
  ensure
    shutdown_dest if count >= 2
  end

  def shutdown_write
    count = dest = nil
    @mutex.synchronize do 
      return if @dest.nil? or @shutting_down
      puts "#{@index}: Shutting down write" if VERBOSE
      dest = @dest
      count = (@close_count += 1)
    end

    dest.flush
    dest.close_write

  rescue Errno::ENOTCONN, Errno::ESHUTDOWN
    # Ignore
  rescue
    puts "#{@index}: shutting down dest: #{$!}", $!.class
  ensure
    shutdown_dest if count >= 2
  end

  def shutdown_dest
    puts "#{@index}: Active #{Time.now - @start} seconds" if @start
    dest = nil
    # puts "#{@index}: shutdown_dest waiting for mutex"
    @mutex.synchronize do 
      return if @dest.nil? or @shutting_down
      puts "#{@index}: Shutting down destination, source still connected" if VERBOSE
      dest, @dest = @dest, nil
    end

    flush_source
    @delegate.shutdown_remote(self)
  end
end

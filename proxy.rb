require 'socket'
require 'thread'

class Proxy
  # A proxy is stale after 10 minutes of non-use
  STALE_PROXY = 600
  
  attr_reader :index, :source, :dest, :dead
  attr_accessor :source_ready, :terminated

  class DestError < StandardError
  end
  
  class LeakError < StandardError
  end

  def set_options(sock)
    nolinger = [1,0].pack "ii"
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_LINGER, nolinger)
    sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)    
  end
  
  def initialize(source, delegate, index)
    @source, @delegate, @index =
      source, delegate, index
    @shutting_down = false
    @dest = nil
    @terminated = @source_ready = @dead = false
    @start = nil
    @close_count = 0
    @pull_state = "NEW"
    @push_state = "NEW"
    @state = "NEW"
    @last_read = @last_write = Time.now

    @mutex = Mutex.new

    set_options(@source)
  end

  def to_s
    "#{@index}: (#{@state}):(#{@pull_state} : #{@push_state})"
  end

  TERMINATOR = 0.chr * 5 + '1818df499987fab124fac0cfc9edb83fa5578c36'
  TERM_LENGTH = TERMINATOR.length
  TERM_RE = Regexp.new(Regexp.quote(TERMINATOR))

  def pull_thread
    begin
      write_closed = false 
      stopped = false
      data = nil
      while !stopped and !@shutting_down and IO.select([@source])
        begin
          @pull_state = "READING"
          block = @source.read_nonblock(1440)
          puts "Pull received: #{block.length}" if VERBOSE
          unless block and !block.empty?
            puts "#{@index}: read returned no data"
            break
          end
          
          @last_read = Time.now
          data ||= ''
          data << block
          next if data.length < TERM_LENGTH
          
          if data =~ TERM_RE
            @pull_state = "LOOKING FOR TERM"
            puts "#{@index}: Received terminator... stopping" if VERBOSE
            stopped = true
            data = $`
          elsif data.index(0.chr)
            puts "Found \\0"  if VERBOSE
            next
          end
          
          begin
            if !write_closed and @dest and data.length > 0
              @pull_state = "WRITING"
              puts "#{@index}: pull Writing #{data.length} bytes"  if VERBOSE
              @dest.write(data) 
              puts "#{@index}: pull Wrote..."  if VERBOSE
              data = nil
            elsif write_closed
              data =  nil
            end
          rescue Errno::EPIPE
            @pull_state = "EPIPE"
            puts "#{@index}: Write closed, stopping pull"
            @delegate.terminate_remote(self)
            write_closed = true
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

    @pull_state = "TERMINATED"
    puts "#{@index}: Pull loop terminated" if VERBOSE
    
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")

  ensure
    if !stopped
      shutdown
    else
      shutdown_write
    end
    @pull_state = "SHUTDOWN"
  end

  def push_thread
    begin
      while @dest and !@shutting_down and IO.select([@dest])
        begin
          data = nil
          @push_state = "WAITING FOR MUTEX"
          @mutex.synchronize do
            break unless @dest
            @push_state = "READING"
            data = @dest.read_nonblock(1440)
          end
          
          unless data and !data.empty?
            @push_state = "READING DONE"
            puts "#{@index}: read returned no data" if VERBOSE
            break
          end
          
          puts "#{@index}: Push received: #{data.length}" if VERBOSE
          @push_state = "WRITING"
          @source.write(data)
          @last_write = Time.now
          
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "#{@index}: EAGAIN (push)" if VERBOSE
        end
      end
      
    rescue Errno::EPIPE, EOFError, Errno::ECONNRESET, Errno::ENOTSOCK, IOError
      puts "#{@index}: Push: Socket closed\n#{$!.backtrace[0]}\n#{$!}\n#{$!.class}" if VERBOSE
      
    rescue
      puts $!, $!.class
      puts $!.backtrace.join("\n")
    end

    @push_state = "TERMINATED"    
    puts "#{@index}: Push terminated" if VERBOSE
    
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
    
  ensure
    @push_state = "SHUTTING DOWN READ"    
    shutdown_read
    @push_state = "SHUTDOWN"    
  end
  
  def create_threads
    @threads = [Thread.new { push_thread },
                Thread.new { pull_thread }]
  end

  def kill_threads
    if @threads
      @push_state = @pull_state = "KILLING THREADS"
      @threads.each do |t|
        puts "#{@index}: Killing thread #{t.inspect}" if VERBOSE
        t.kill unless Thread.current == t
      end
    end
  end

  def send_terminator(force = false)
    @push_state = "SENDING TERMINATOR"
    puts "#{@index}: send_terminator: Waiting for mutex" if VERBOSE
    @mutex.synchronize do
      if @terminated and !force
        @push_state = "ALREADY TERMINATED"
        return
      end
      @terminated = true
    end
    
    @push_state = "WRITING TERMINATOR"
    puts "#{@index}: Writing terminator..." if VERBOSE
    @source.write(TERMINATOR)
    @push_state = "FLUSHING SOURCE"
    @source.flush
            
  rescue Errno::ESHUTDOWN
    puts "Socket already closed"
  end

  def dest=(dest)
    @pull_state = @push_state = "RESETTING DEST"
    @mutex.synchronize do
      if @dest
        @pull_state = @push_state = "DEST ERROR"
        raise DestError, "#{@index}: Can't set dest when one is already present" if VERBOSE
      end
      # puts "#{@index}: Setting dest to: #{dest}"
      @pull_state = @push_state = "DEST SET"
      @dest = dest
    end

    @source_ready = false
    @terminated = false
    @dead = false
    @close_count = 0

    @state = "OPERATIONAL"
    set_options(@dest)
    @pull_state = @push_state = "CREATING THREADS"
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

    if @dest
      begin
        puts "#{@index}: Closing dest socket: #{$!}" if VERBOSE
        @dest.close
      rescue Exception
        puts "#{@index}: Destination cannot be closed: #{$!}"
      ensure
        @dest = nil
      end
    else
      puts "#{@index}: Cannot close socket, dest is nil!" if VERBOSE
    end

    begin
      puts "#{@index}: Closing source" if VERBOSE
      @source.close
    rescue
      puts "#{@index}: Source cannot be closed: #{$!}"
    ensure
      @source = nil
    end
    
    @dead = true
    @delegate.shutdown_remote(self)
    @state = "DEAD"
    
  rescue
    puts "Shutdown failed..."
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end

  def flush_source
    puts "#{@index}: Flushing source..." if VERBOSE
    while data = @source.read_nonblock(1440)
      puts "Flushed: #{data.inspect}" if data != TERMINATOR
      break if data.empty?
    end
  rescue Errno::EAGAIN, Errno::EWOULDBLOCK, EOFError
  end

  def shutdown_read
    count = dest = nil
    @push_state = "SHUTDOWN READ (waiting)"
    @mutex.synchronize do 
      return if @dest.nil? or @shutting_down
      puts "#{@index}: Shutting down read" if VERBOSE
      dest = @dest
      count = (@close_count += 1)
    end

    puts "#{@index}: shutting down read side of proxy" if VERBOSE
    begin
      @push_state = "SHUTDOWN READ (closing)"
      dest.close_read
    rescue Errno::ENOTCONN, Errno::ESHUTDOWN
      puts "#{@index}: Close read: #{$!}" if VERBOSE
    end
    
    @push_state = "SENDING TERMINATOR"
    send_terminator

  rescue Errno::ENOTCONN, Errno::ESHUTDOWN, Errno::IOError
    # Ignore
  rescue
    puts "#{@index}: shutting down read: #{$!}"
  ensure
    shutdown_dest if count and count >= 2
  end

  def shutdown_write
    count = dest = nil
    @pull_state = "SHUTTING DOWN WRITE (wait)"
    @mutex.synchronize do 
      return if @dest.nil? or @shutting_down
      dest = @dest
      count = (@close_count += 1)
    end

    puts "#{@index}: Shutting down write" if VERBOSE
    
    @pull_state = "SHUTTING DOWN WRITE (close)"
    dest.flush
    dest.close_write

  rescue Errno::ENOTCONN, Errno::ESHUTDOWN, Errno::IOError
    # Ignore
  rescue
    puts "#{@index}: shutting down write: #{$!}"
  ensure
    shutdown_dest if count and count >= 2
  end

  def shutdown_dest
    puts "#{@index}: Active #{Time.now - @start} seconds" if VERBOSE and @start
    dest = nil
    # puts "#{@index}: shutdown_dest waiting for mutex"
    @state = "SHUTDOWN - waiting"
    @mutex.synchronize do 
      return if @dest.nil? or @shutting_down
      puts "#{@index}: Shutting down destination, source still connected" if VERBOSE
      dest, @dest = @dest, nil
    end

    @state = "SHUTDOWN - flushing"
    flush_source
    @delegate.shutdown_remote(self)
    @state = "SHUTDOWN"
  end
  
  # Determine if this proxy connection is stale. We will reap stale connections since
  # they might be broken
  def old?
    now = Time.now
    (now - @last_read) >= STALE_PROXY && (now - @last_write) >= STALE_PROXY
  end
end

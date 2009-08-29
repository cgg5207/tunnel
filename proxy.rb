require 'socket'
require 'thread'

class Proxy
  attr_reader :index, :source, :dest

  @@mutex = Mutex.new
  
  def set_options(sock)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
    sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)    
  end
  
  def initialize(source, delegate, index)
    @source, @delegate, @index =
      source, delegate, index
    @shutting_down = false
    @dest = nil

    set_options(@source)
  end

  TERMINATOR = '~|~|~|~|~|~5818df499987fab124fac0cfc9edb83fa5578c36'
  TERM_LENGTH = TERMINATOR.length
  TERM_RE = Regexp.new(Regexp.quote(TERMINATOR))

  def pull_thread
    stopped = false
    begin
      data = nil
      while !@shutting_down and IO.select([@source])
        begin
          block = @source.read_nonblock(512)
          unless block and !block.empty?
            puts "read returned no data"
            break
          end
          
          # puts "Received #{r.addr[2]}: #{block.length}"
          data ||= ''
          data << block
          next if data.length < TERM_LENGTH
          
          if data =~ TERM_RE
            puts "Received terminator... stopping"
            stopped = true
            data = $`
          else
            next if data.index(?~)
          end

          @dest.write(data) if @dest
          data = nil
          
          break if stopped

        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "EAGAIN"
        end
      end
    
    rescue Errno::EPIPE, EOFError, Errno::ECONNRESET
      puts "Pull: socket closed"
    rescue
      puts $!, $!.class
      puts $!.backtrace.join("\n")
    end

    puts "Pull loop terminated"
    
    shutdown if !stopped

  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end

  def push_thread
    begin
      while @dest and !@shutting_down and IO.select([@dest])
        begin
          data = @dest.read_nonblock(512)
          unless data and !data.empty?
            puts "read returned no data"
            break
          end
          
          @source.write(data)
          
        rescue Errno::EAGAIN, Errno::EWOULDBLOCK
          puts "EAGAIN"
        end
      end
      
    rescue Errno::EPIPE, EOFError, Errno::ECONNRESET
      puts "Push: Socket closed"
    rescue
      puts $!, $!.class
      puts $!.backtrace.join("\n")
    end

    puts "Push terminated"
    
    send_terminator
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

  def send_terminator
    puts "Writing terminator..."
    @source.write(TERMINATOR)
    @source.flush
            
  rescue Errno::ESHUTDOWN
    puts "Socket already closed"
  end

  def dest=(dest)
    if @dest
      raise "Can't set dest when one is already present"
    end

    puts "Setting dest to: #{dest}"

    @dest = dest
    set_options(@dest)
    create_threads
  end
  

  def shutdown
    unless @shutting_down
      puts "Shutting down proxy for #{self.index}"
      @shutting_down = true
      @source.shutdown rescue
      if @dest
        begin
          @dest.shutdown
        rescue
        end
      end
      
      @delegate.shutdown_remote(self)
    end
    
  rescue
    puts $!
    puts $!.backtrace.join("\n")
  end

  def flush_source
    puts "Flushing source..."
    while data = @source.read_nonblock(512)
      puts data.inspect
    end
  rescue Errno::EAGAIN, Errno::EWOULDBLOCK, EOFError
  end

  def shutdown_dest
    @@mutex.synchronize do 
      if @dest and !@shutting_down
        puts "Shutting down destination #{self.index}, source still connected"
        dest, @dest = @dest, nil
        dest.shutdown rescue
        
        flush_source
        
        puts "Shutdown dest calling delegate for #{self.index}"
        @delegate.shutdown_remote(self)
      end
    end

      
  rescue Errno::ENOTCONN, Errno::ESHUTDOWN
    # puts "Ignoring not connected"
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end
end

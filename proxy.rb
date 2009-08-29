require 'socket'
require 'thread'

class Proxy
  attr_accessor :index
  
  @@mutex = Mutex.new
  
  def set_options(sock)
    sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)
    sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, true)    
  end
  
  def initialize(source, dest, delegate, index)
    @source, @dest, @delegate, @index =
      source, dest, delegate, index
    @shutting_down = false

    set_options(@source)
    set_options(@dest)

    create_threads
  end

  TERMINATOR = '~|~|~|~|~|~5818df499987fab124fac0cfc9edb83fa5578c36'
  TERM_LENGTH = TERMINATOR.length
  TERM_RE = Regexp.new(Regexp.quote(TERMINATOR))

  def create_threads
    @threads = [[@source, @dest], [@dest, @source]].map do |r, w|
      Thread.new do
        begin
          data = nil
          while @dest and !@shutting_down and IO.select([r])
            begin
              block = r.read_nonblock(512)
              unless block and !block.empty?
                puts "read returned no data"
                break
              end
              puts "Received #{r.addr[2]}: #{block.length}"
              if block.length < TERM_LENGTH
                data ||= ''
                data << block
                next
              elsif data
                data << block
              else
                data = block
              end
              
              if data =~ TERM_RE
                puts "Received terminator... stopping"
                stopped = true
                data = $`
              end
              
              w.write(data)
              data = nil
              
              break if stopped
              
            rescue Errno::EAGAIN
            end
          end

        rescue Errno::EPIPE, EOFError, Errno::ECONNRESET
          puts "Socket closed"
        rescue
          puts $!, $!.class
          puts $!.backtrace.join("\n")
        end

        begin
          @@mutex.synchronize do
            if r == @source and !stopped
              shutdown
            else
              puts "Writing terminator..."
              w.flush
              w.write(TERMINATOR)
              w.flush

              shutdown_dest
            end
            # kill_threads
          end
        rescue
          puts $!, $!.class
          puts $!.backtrace.join("\n")
        end
      end
    end
  end

  def kill_threads
    if @threads
      threads, @threads = @threads, nil
      threads.each do |t|
        t.kill unless Thread.current == t
      end
    end
  end

  def reset_dest(dest)
    if @dest
      raise "Can't set dest when one is already present"
    end

    @dest = dest
    set_options(@dest)
    create_threads
  end
  

  def shutdown
    unless @shutting_down
      puts "Shutting down proxy for #{self.index} #{@source.addr.inspect}"
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

  def shutdown_dest
    if @dest and !@shutting_down
      puts "Shutdown dest calling delegate for #{self.index}"
      @delegate.shutdown_remote(self)
      
      puts "Shutting down destination #{@dest.addr.inspect}, source still connected"
      dest, @dest = @dest, nil
      dest.shutdown

      puts "Flushing source..."
      begin
        while data = @source.read_nonblock(512)
          puts data
        end
      rescue Errno::EAGAIN
      end
    end

  rescue Errno::ENOTCONN
    # puts "Ignoring not connected"
  rescue
    puts $!, $!.class
    puts $!.backtrace.join("\n")
  end
end

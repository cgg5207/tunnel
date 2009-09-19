require 'rubygems'
require 'daemons'

Daemons.run('proxy_server.rb',
            :app_name => 'proxy_server',
            :dir => '.',
            :dir_mode => :script,
            :backtrace => true,
            :log_output => true,
            :monitor => true)

#!/usr/bin/env ruby

require 'time'
require 'yaml'
require 'benchmark'
require 'logger'

require 'bundler'
Bundler.require(:default)


class Database
  def initialize(env)
    conf = YAML.load_file(File.dirname(__FILE__) + '/database.yml')[env]
    @client = Mysql2::Client.new(conf.inject({}){|memo,(k,v)| memo[k.to_sym] = v; memo})
    @formatter = AnbtSql::Formatter.new(AnbtSql::Rule.new)
  end

  def processlist
    result = @client.query "SHOW FULL PROCESSLIST"
    result.to_a
  end
end

class Formatter

  attr_accessor :names, :datas

  def initialize
    @names = []
    @datas = []
    @query = []
    @buffer = ""
  end

  def humanize_sec(secs)
    [[60, :s], [60, :m], [24, :h], [1000, :d]].map{ |count, name|
      if secs > 0
        secs, n = secs.divmod(count)
        "#{n.to_i}#{name}"
      end
    }.compact.reverse.join('')
  end

  def query=(q)
    formatter = AnbtSql::Formatter.new(AnbtSql::Rule.new)
    if q
      @query << "Query:"
      @query << ""
      formatter.format(q).split("\n").each do |row|
        if row.length < 200
          @query << row
        else
          row.scan(/.{1,200}\W/).each do |r|
            @query << r
          end
        end
      end
      #@query.flatten!
    end
  end

  def table
    total_width = [@names.length, @datas.length, @query.length].max
    (0...total_width).each do |i|
      @buffer << ("%20s" % @names[i]).yellow
      if @names[i] 
        @buffer << " : "
      else 
        @buffer << "   "
      end
      @buffer << ("%-20s" % @datas[i]).green
      @buffer << " | "
      @buffer << ("%-200s" % @query[i]).blue
      @buffer << "\n"
    end
    @buffer << "\n"
    @buffer
  end
end

db = Database.new('development')

loop do
  puts "\e[H\e[2J"
  puts "Active Mysql processes (#{Time.now.to_s.green})".yellow
  puts

  db.processlist.each do |row|

    formatter = Formatter.new

    #next if row['Command'] == 'Sleep'
    #next if row['Info'] == 'SHOW FULL PROCESSLIST'  

    formatter.query = row['Info'] if row['Info']  

    if row['db'] && row['db'].length > 0
      formatter.names << "Database"
      formatter.datas << row['db']
    end

    if row['User'] && row['User'].length > 0
      formatter.names << "User"
      formatter.datas << row['User']
    end

    if row['Host'] && row['Host'].length > 0
      formatter.names << "Host"
      formatter.datas << row['Host']
    end

    if row['State'] && row['State'].length > 0
      formatter.names << "State"
      formatter.datas << row['State']
    end

    if row['Time'] && row['Time'] > 0
      formatter.names << "Time"
      formatter.datas << formatter.humanize_sec(row['Time'])
    end

    if row['Command'] && row['Command'].length > 0
      formatter.names << "Command"
      formatter.datas << row['Command']
    end

    puts formatter.table

  end
  
  sleep 0.1

end

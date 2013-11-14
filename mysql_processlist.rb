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

  def print_table
    total_width = [@names.length, @datas.length, @query.length].max
    (0...total_width).each do |i|
      print ("%20s" % @names[i]).yellow
      if @names[i] 
        print " : "
      else 
        print "   "
      end
      print ("%-20s" % @datas[i]).green
      print " | "
      print ("%-200s" % @query[i]).blue
      print "\n"
    end
    puts
  end
end

puts "Active Mysql processes:".yellow
puts

db = Database.new('development')

db.processlist.each do |row|

  formatter = Formatter.new
  
  next if row['Command'] == 'Sleep'
  next if row['Info'] == 'SHOW FULL PROCESSLIST'  
 
  formatter.query = row['Info'] if row['Info']  

  if row['db']
    formatter.names << "Database"
    formatter.datas << row['db']
  end
 
  if row['User']
    formatter.names << "User"
    formatter.datas << row['User']
  end
  
  if row['Host']
    formatter.names << "Host"
    formatter.datas << row['Host']
  end
  
  if row['State']
    formatter.names << "State"
    formatter.datas << row['State']
  end
  
  if row['Time']
    formatter.names << "Time"
    formatter.datas << formatter.humanize_sec(row['Time'])
  end
  
  if row['Command']
    formatter.names << "Command"
    formatter.datas << row['Command']
  end

  formatter.print_table

end

require 'rubygems'
require 'bundler/setup'
require 'gooddata'
require 'mail'
require 'net/http'
require 'net/imap'
require 'optparse'
require 'uri'
require 'syslog/logger'
require 'newrelic_rpm'

NewRelic::Agent.manual_start

logger = Syslog::Logger.new 'gooddata-writer'

options = {}
optparse = OptionParser.new do |opts|
  opts.banner = 'Usage: run.rb [options]'
  opts.on('-gu', '--gd_username USERNAME', 'GoodData username') { |v| options[:gd_username] = v }
  opts.on('-gp', '--gd_password PASSWORD', 'GoodData password') { |v| options[:gd_password] = v }
  opts.on('-ep', '--email_username USERNAME', 'Email username') { |v| options[:email_username] = v }
  opts.on('-ep', '--email_password PASSWORD', 'Email password') { |v| options[:email_password] = v }
  opts.on('-eh', '--host HOST', 'Email host') { |v| options[:host] = v }
  opts.on('-ehp', '--port PORT', 'Email host port') { |v| options[:port] = v }
end.parse!

begin
  optparse.parse!
  mandatory = [:gd_username, :gd_password, :email_username, :email_password]
  missing = mandatory.select{ |param| options[param].nil? }
  unless missing.empty?
    puts "Missing options: #{missing.join(', ')}"
    puts optparse
    exit
  end
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $!.to_s
  puts optparse
  exit
end

if options[:host] == nil
  options[:host] = 'imap.gmail.com'
end
if options[:port] == nil
  options[:port] = 993
end

def fetch(uri_str, limit = 10)
  return if limit == 0
  p uri_str
  response = Net::HTTP.get_response(URI(uri_str))
  case response
    when Net::HTTPSuccess then
      response
    when Net::HTTPRedirection then
      location = response['location']
      fetch(location, limit - 1)
    else
      response.value
  end
end

def run
  repeat = 0
  last_error = nil
  start_time = Time.now
  begin
    begin
      imap = Net::IMAP.new options[:host], options[:port], true, nil, false
      imap.login options[:email_username], options[:email_password]

      imap.select 'INBOX'
      imap.search(['NOT', 'SEEN']).each do |message_id|
        catch :invitation_ok do
          envelope = imap.fetch(message_id, 'ENVELOPE')[0].attr['ENVELOPE']
          if envelope.from[0]['host'] == 'gooddata.com' && envelope.from[0]['mailbox'] == 'invitation'
            message = imap.fetch(message_id, 'RFC822')[0].attr['RFC822']
            mail = Mail.read_from_string message
            mail.body.decoded.split(' ').each { |mail_part|
              if mail_part =~ URI::regexp && mail_part.start_with?('https://secure.gooddata.com')
                invitation_id = mail_part.split('/').last

                GoodData.connect options[:gd_username], options[:gd_password]
                invitation = GoodData.get '/gdc/account/invitations/' + invitation_id

                if invitation['invitation']['content']['status'] == 'ACCEPTED'
                  throw :invitation_ok
                end

                result = {
                    'pid' => invitation['invitation']['links']['project'].split('/').last,
                    'sender' => invitation['invitation']['meta']['author']['email'],
                    'createDate' => invitation['invitation']['meta']['created']
                }

                begin
                  GoodData.post '/gdc/account/invitations/' + invitation_id, {
                      'invitationStatusAccept' => {
                          'status' => 'ACCEPTED'
                      }
                  }
                    result['status'] = 'ok'
                rescue Exception => e
                  result['status'] = 'error'
                  result['error'] = e.message
                end

                puts JSON.generate(result)

                imap.store message_id, '+FLAGS', [:Seen]
              end
            }
          end
        end
      end
      imap.logout
      imap.disconnect
      repeat += 1
      sleep 10

    rescue StandardError, Net::IMAP::IOError => e
      last_error = e
      begin
        imap.disconnect if imap
      rescue IOError => e
        #ignore
      end
      next
    end
  end while ((Time.now - start_time) < 300)

  if repeat == 0
    if last_error
      raise last_error
    else
      raise 'Could not connect to IMAP without error message'
    end
  end
end

begin
  run
rescue Exception
  error = { :message => $!.to_s, :trace => $@, :app => 'syrup', :component => 'gooddata-writer', :priority => 'CRITICAL' }
  logger.fatal error.to_json
end
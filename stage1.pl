#!/usr/bin/perl
# parse haproxy logs and put out key value pair with store as key
# value includes data that is not haproxy specific first ie: request related

use Time::Local;

while(<>) {
 my $logline = $_;
 chomp($logline);
 my $type = "";
 my @tokens = split( /\s+/, $_);
 #Ignore tokens 0-4 as these are syslog-ng information

 # 5 is client_ip:source_port 
 my @client_data = split( /:/, $tokens[5]);
 my $client_ip = $client_data[0];

 # 6 date and convert to EPOCH
 if ( $tokens[6] =~ m/\[([0-9]{2})\/([a-zA-z]+)\/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]+)/ ) {
	my $day = $1;
	my $month = $2;
	my $year = $3;
	my $hour = $4;
	my $min = $5;
	my $sec = $6;
	$msec = $7;
	$epoch_time = timelocal($sec, $min, $hour, $day, $month, $year);
	$human_date = "$year-$month-$day";
	$human_time = "$hour:$min:$sec.$msec";
 }

 # 7 haproxy frontend
 my $frontend = $tokens[7];

 # 8 haproxy backend and server separated by /
 my @backend_server = split( /\//, $tokens[8]);
 my $backend = $backend_server[0]; 
 my $server = $backend_server[1]; 

 # 9 haproxy timing data separated by /
 my @haproxy_timings = split( /\//, $tokens[9]);
 my $tq = $haproxy_timings[0];
 my $tw = $haproxy_timings[1];
 my $tc = $haproxy_timings[2];
 my $tr = $haproxy_timings[3];
 my $tt = $haproxy_timings[4];

 # 10 HTTP reponse code
 my $reponse_code = $tokens[10];

 # 11 Number of bytes returned
 if ( length($tokens[11]) != 0 ) {
     $bytes = $tokens[11];
 } else {
     $bytes = 0;
 }

 # 12 captured_request_cookie
 # 13 captured_response_cookie
 # 14 termination_state
 $term_state = $tokens[14];
 if ( $term_state =~ m/^[Ss].{3}/g ) {
	$term_state = "server_error";
 } else {
	$term_state = "ok";
 }

 # 15 actconn '/' feconn '/' beconn '/' srv_conn '/' retries*
 # 16 srv_queue '/' backend_queue

 # 17 uniq ID
 my $uniq_id = $tokens[17];

 # 18 HTTP method
 my $method = $tokens[18];
 $method =~ s/\"//;

 # 19 uri
 my $uri = $tokens[19];

 my @uri_parts = split( /\//, $uri);
 my $store = $uri_parts[2];


 if ( $uri_parts[3] eq "services" ) {
    if ( $uri_parts[4] =~ m/facet|sparql|augment|oai-pmh|multisparql|jobs|config/ )
    {
      $type = $&;
    }
 } 

 if ( $uri_parts[3] eq "snapshots" ) {
      $type = $uri_parts[3];
 } 

 if ( $uri_parts[3] =~ m/items/ )
 {
   if ( $uri_parts[3] =~ m/items\?query/ )
   {
      $type = "items_query";
   } else {
      $type = "items";
   }
 }
 
 if ( $uri_parts[3] =~ m/meta/ )
 {
   if ( $uri_parts[3] =~ m/meta\?about/ )
   {
      $type = "meta_describe";
   } else {
      $type = "meta";
   }
 }
 
 if ( $type ne "" )
 {
   print "$store\t$type,$reponse_code,$tt,$bytes,$epoch_time\.$msec,$human_date,$method,$uniq_id,$client_ip,$frontend,$backend,$server,$term_state,$uri\n";
 } else {
   #print "NOT MATCHED: $logline\n";
 }
}

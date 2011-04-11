#!/usr/bin/perl
# parse haproxy logs and put out key value pair with store as key
# value includes data that is not haproxy specific first ie: request related

use Time::Local;

while(<>) {
 my $logline = $_;
 chomp($logline);
 my $type = "";
 my @tokens = split( /\s+/, $_);
 my @client_data = split( /:/, $tokens[5]);
 my $client_ip = $client_data[0];
 # get date and convert to EPOCH
 if ( $tokens[6] =~ m/\[([0-9]{2})\/([a-zA-z]+)\/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2})\.([0-9]+)/ ) {
	my $day = $1;
	my $month = $2;
	my $year = $3;
	my $hour = $4;
	my $min = $5;
	my $sec = $6;
	$msec = $7;
	$epoch_time = timelocal($sec, $min, $hour, $day, $month, $year);
	$human_time = "$year-$month-$day $hour:$min:$sec.$msec";
 }
 my $frontend = $tokens[7];
 my @backend_server = split( /\//, $tokens[8]);
 my $backend = $backend_server[0]; 
 my $server = $backend_server[1]; 
 my @haproxy_timings = split( /\//, $tokens[9]);
 my $response_time = $haproxy_timings[4];
 my $reponse_code = $tokens[10];
 my $bytes = $tokens[11];
 my $uniq_id = $tokens[17];
 my $method = $tokens[18];
 my $uri = $tokens[19];
 my @uri_parts = split( /\//, $uri);
 my $store = $uri_parts[2];

 $method =~ s/\"//;

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
   print "$store\t$type,$reponse_code,$response_time,$bytes,$epoch_time\.$msec,$human_time,$method,$uniq_id,$client_ip,$frontend,$backend,$server,$uri\n";
 } else {
   #print "NOT MATCHED: $logline\n";
 }
}

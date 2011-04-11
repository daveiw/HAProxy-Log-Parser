#!/usr/bin/perl

use Switch;

sub pad {
  my ($num)=@_;
  if (length($num) < 2 ) {
    $num = "0$num";
  }
  return($num);
}

$start=localtime(time);
($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time - 86400);
$today = pad($mday);
$year+=1900;
$mon++;
$mon = pad($mon);
$mday = pad($mday);
$hour = pad($hour);
$min = pad($min);

while(<>) {
 my $logline = $_;
 my @tokens = split( /\s+/, $_);
 my $store = $tokens[0];
 my @vals = split(/,/, $tokens[1]);
 my $type = $vals[0];
 my $rtime = ( $vals[2] * 1000 );
 my $bytes = $vals[3];

 #print "DEBUG: $store TYPE:$type TIME:$rtime BYTES:$bytes\n";

 # Genral overall counters
 $bytes{$store}+= $bytes;
 $hit{$store}++;
 $rtime{$store}+= $rtime;
 $total_requests++;
 $total_bytes+= $bytes;
 $total_time+= $rtime;

    switch ( $type ) { case /items/	{
					  $items_r++;
					  $items_b+=$bytes;
					  $items_t+=$rtime;
                                          $items{$store}{requests}++;
                                          $items{$store}{bytes}+= $bytes;
                                          $items{$store}{time}+= $rtime;
                                          $items{$store}{ave} = int($items{$store}{time} / $items{$store}{requests});
                                          if ( $rtime > $items{$store}{time_max} ) {
                                            $items{$store}{time_max} = $rtime;
                                          }
                                          if ( $items{$store}{time_min}<= 0 )
                                          {
                                            $items{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $items{$store}{time_min} )
                                          {
                                            $items{$store}{time_min} = $rtime;
                                          }
					}
                      case /meta/	{
					  $meta_r++;
					  $meta_b+=$bytes;
					  $meta_t+=$rtime;
                                          $meta{$store}{requests}++;
                                          $meta{$store}{bytes}+= $bytes;
                                          $meta{$store}{time}+= $rtime;
                                          $meta{$store}{ave} = int($meta{$store}{time} / $meta{$store}{requests});
                                          if ( $rtime > $meta{$store}{time_max} ) {
                                            $meta{$store}{time_max} = $rtime;
                                          }
                                          if ( $meta{$store}{time_min}<= 0 )
                                          {
                                            $meta{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $meta{$store}{time_min} )
                                          {
                                            $meta{$store}{time_min} = $rtime;
                                          }
					}
                      case /sparql/	{
					  $sparql_r++;
					  $sparql_b+=$bytes;
					  $sparql_t+=$rtime;
                                          $sparql{$store}{requests}++;
                                          $sparql{$store}{bytes}+= $bytes;
                                          $sparql{$store}{time}+= $rtime;
                                          $sparql{$store}{ave} = int($sparql{$store}{time} / $sparql{$store}{requests});
                                          if ( $rtime > $sparql{$store}{time_max} ) {
                                            $sparql{$store}{time_max} = $rtime;
                                          }
                                          if ( $sparql{$store}{time_min}<= 0 )
                                          {
                                            $sparql{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $sparql{$store}{time_min} )
                                          {
                                            $sparql{$store}{time_min} = $rtime;
                                          }
					}
                      case /multisparql/	{
					  $multisparql_r++;
					  $multisparql_b+=$bytes;
					  $multisparql_t+=$rtime;
                                          $multisparql{$store}{requests}++;
                                          $multisparql{$store}{bytes}+= $bytes;
                                          $multisparql{$store}{time}+= $rtime;
                                          $multisparql{$store}{ave} = int($multisparql{$store}{time} / $multisparql{$store}{requests});
                                          if ( $rtime > $multisparql{$store}{time_max} ) {
                                            $multisparql{$store}{time_max} = $rtime;
                                          }
                                          if ( $multisparql{$store}{time_min}<= 0 )
                                          {
                                            $multisparql{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $multisparql{$store}{time_min} )
                                          {
                                            $multisparql{$store}{time_min} = $rtime;
                                          }
					}
                      case /facet/	{
					  $facet_r++;
					  $facet_b+=$bytes;
					  $facet_t+=$rtime;
                                          $facet{$store}{requests}++;
                                          $facet{$store}{bytes}+= $bytes;
                                          $facet{$store}{time}+= $rtime;
                                          $facet{$store}{ave} = int($facet{$store}{time} / $facet{$store}{requests});
                                          if ( $rtime > $facet{$store}{time_max} ) {
                                            $facet{$store}{time_max} = $rtime;
                                          }
                                          if ( $facet{$store}{time_min}<= 0 )
                                          {
                                            $facet{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $facet{$store}{time_min} )
                                          {
                                            $facet{$store}{time_min} = $rtime;
                                          }
					}
                      case /oai/	{
					  $oai_r++;
					  $oai_b+=$bytes;
					  $oai_t+=$rtime;
                                          $oai{$store}{requests}++;
                                          $oai{$store}{bytes}+= $bytes;
                                          $oai{$store}{time}+= $rtime;
                                          $oai{$store}{ave} = int($oai{$store}{time} / $oai{$store}{requests});
                                          if ( $rtime > $oai{$store}{time_max} ) {
                                            $oai{$store}{time_max} = $rtime;
                                          }
                                          if ( $oai{$store}{time_min}<= 0 )
                                          {
                                            $oai{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $oai{$store}{time_min} )
                                          {
                                            $oai{$store}{time_min} = $rtime;
                                          }
					}
                      case /augment/	{
					  $augment_r++;
					  $augment_b+=$bytes;
					  $augment_t+=$rtime;
                                          $augment{$store}{requests}++;
                                          $augment{$store}{bytes}+= $bytes;
                                          $augment{$store}{time}+= $rtime;
                                          $augment{$store}{ave} = int($augment{$store}{time} / $augment{$store}{requests});
                                          if ( $rtime > $augment{$store}{time_max} ) {
                                            $augment{$store}{time_max} = $rtime;
                                          }
                                          if ( $augment{$store}{time_min}<= 0 )
                                          {
                                            $augment{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $augment{$store}{time_min} )
                                          {
                                            $augment{$store}{time_min} = $rtime;
                                          }
					}
                      case /snapshot/	{
					  $snapshot_r++;
					  $snapshot_b+=$bytes;
					  $snapshot_t+=$rtime;
                                          $snapshot{$store}{requests}++;
                                          $snapshot{$store}{bytes}+= $bytes;
                                          $snapshot{$store}{time}+= $rtime;
                                          $snapshot{$store}{ave} = int($snapshot{$store}{time} / $snapshot{$store}{requests});
                                          if ( $rtime > $snapshot{$store}{time_max} ) {
                                            $snapshot{$store}{time_max} = $rtime;
                                          }
                                          if ( $snapshot{$store}{time_min}<= 0 )
                                          {
                                            $snapshot{$store}{time_min} = $rtime;
                                          }
                                          if ( $rtime < $snapshot{$store}{time_min} )
                                          {
                                            $snapshot{$store}{time_min} = $rtime;
                                          }
					}
		    }

    #print "Items Count:$items{$store}{requests} Ave Response:$items{$store}{ave} MAX:$items{$store}{time_max} MIN:$items{$store}{time_min}\n";
    #print "Meta Count:$meta{$store}{requests} Ave Response:$meta{$store}{ave} MAX:$meta{$store}{time_max} MIN:$meta{$store}{time_min}\n";
    #print "SPARQL Count:$sparql{$store}{requests} Ave Response:$sparql{$store}{ave} MAX:$sparql{$store}{time_max} MIN:$sparql{$store}{time_min}\n";

}

#Ouput report
$basedir = "/var/backups/storestats";
open(REQUESTS, ">> $basedir/csvdata/daily-requests-$year-$mon-$mday.csv");
open(BYTES, ">> $basedir/csvdata/daily-bytes-$year-$mon-$mday.csv");
open(TIME, ">> $basedir/csvdata/daily-time-$year-$mon-$mday.csv");

#Prints column headers
print REQUESTS "Storename,Total Requests,Items Requests,Meta Requests,Sparql Requests,Multisparql Requests,Augment Requests,Facet Requests,Oai Requests,Snapshot Requests\n";
print BYTES "Storename,Total Bytes,Items Bytes,Meta Bytes,Sparql Bytes,Multisparql Bytes,Augment Bytes,Facet Bytes,Oai Bytes,Snapshot Bytes\n";
print TIME "Storename,Total Time,Items Time,Meta Time,Sparql Time,Multisparql Time,Augment Time,Facet Time,Oai Time,Snapshot Time\n";
#print "Storename,Total Requests,Items Requests,Meta Requests,Sparql Requests,Multisparql Requests,Augment Requests,Facet Requests,Oai Requests,Snapshot Requests\n";

#Prints Totals
print REQUESTS "Totals,$total_requests,$items_r,$meta_r,$sparql_r,$multisparql_r,$augment_r,$facet_r,$oai_r,$snapshot_r\n";
print BYTES "Totals,$total_bytes,$items_b,$meta_b,$sparql_b,$multisparql_b,$augment_b,$facet_b,$oai_b,$snapshot_b\n";
print TIME "Totals,$total_time,$items_t,$meta_t,$sparql_t,$multisparql_t,$augment_t,$facet_t,$oai_t,$snapshot_t\n";

#Print Store stats
foreach ( sort { $hit{$b} <=> $hit{$a} } keys %hit ) {
  print REQUESTS "$_,$hit{$_},$items{$_}{requests},$meta{$_}{requests},$sparql{$_}{requests},$multisparql{$_}{requests},$augment{$_}{requests},$facet{$_}{requests},$oai{$_}{requests},$snapshot{$_}{requests}\n";
  print BYTES "$_,$bytes{$_},$items{$_}{bytes},$meta{$_}{bytes},$sparql{$_}{bytes},$multisparql{$_}{bytes},$augment{$_}{bytes},$facet{$_}{bytes},$oai{$_}{bytes},$snapshot{$_}{bytes}\n";
  print TIME "$_,$rtime{$_},$items{$_}{time},$meta{$_}{time},$sparql{$_}{time},$multisparql{$_}{time},$augment{$_}{time},$facet{$_}{time},$oai{$_}{time},$snapshot{$_}{time}\n";

  #print "$_,$hit{$_},$items{$_}{requests},$meta{$_}{requests},$sparql{$_}{requests},$multisparql{$_}{requests},$augment{$_}{requests},$facet{$_}{requests},$oai{$_}{requests},$snapshot{$_}{requests}\n";
}

close(REQUESTS);
close(BYTES);
close(TIME);

$finish_time=localtime(time);
open(OUT, "> $basedir/tools/totals");
print OUT "Daily Platform stats results\nProcessed $total_requests log lines\nStart Time:$start\nEnd Time:$finish_time\n";
close(OUT);

#!/usr/bin/perl

while(<>) {
 my $logline = $_;
 my @tokens = split( /\s+/, $_);
 my $store = $tokens[0];
 my @vals = split(/,/, $tokens[1]);
 my $type = $vals[0];
 my $code = $vals[1];
 my $time = $vals[2];
 my $bytes = $vals[3];
 $date = $vals[4];
  
 push @{ $requests{$store}{$type}{$code}{percentile} }, $time;

 $requests{$store}{$type}{$code}{requests}++;
 $requests{$store}{$type}{$code}{total_time}+=$time;
 if ( $time > $requests{$store}{$type}{$code}{max_time} ) {
   $requests{$store}{$type}{$code}{max_time} = $time;
 }
 if ( $requests{$store}{$type}{$code}{min_time} == '' ) {
   $requests{$store}{$type}{$code}{min_time} = $time;
 } elsif ( $requests{$store}{$type}{$code}{min_time} > $time ) {
   $requests{$store}{$type}{$code}{min_time} = $time;
 }
}

for $store ( keys %requests ) {
  for $type ( keys %{ $requests{$store} } ) {
    for $code ( keys %{ $requests{$store}{$type} } ) {
       for $val ( keys %{ $requests{$store}{$type}{$code} } ) {
         if ( ref($requests{$store}{$type}{$code}{$val}) eq 'ARRAY' ) { 
	   $num_times = $#{ $requests{$store}{$type}{$code}{$val} };
	   #Calc 95th percentile
	   $n95 = ( 0.95 * $num_times );
	   $n75 = ( 0.75 * $num_times );
	   $n50 = ( 0.5 * $num_times );
	   $n25 = ( 0.25 * $num_times );
	   $p95 = sprintf("%.0f", $n95 );
	   $p75 = sprintf("%.0f", $n75 );
	   $p50 = sprintf("%.0f", $n50 );
	   $p25 = sprintf("%.0f", $n25 );
	   @timings = sort {$a <=> $b} @{$requests{$store}{$type}{$code}{$val}};
           print "$date\t$store\t$type\t$code\t95thPercentile\t$timings[$p95]\n";
           print "$date\t$store\t$type\t$code\t75thPercentile\t$timings[$p75]\n";
           print "$date\t$store\t$type\t$code\t50thPercentile\t$timings[$p50]\n";
           print "$date\t$store\t$type\t$code\t25thPercentile\t$timings[$p25]\n";
         } else {
           print "$date\t$store\t$type\t$code\t$val\t$requests{$store}{$type}{$code}{$val}\n";
         }
       }
    }
  }
}

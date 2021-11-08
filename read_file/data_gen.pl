#!/usr/bin/perl
use strict;
use warnings;
use JSON::XS;
use Data::Dumper qw(Dumper);
use AnyEvent;
use EV;
use AE;


main();

sub main {
    my $format = read_format();
    my $nb_line = $ARGV[0];

    write_file($format, $nb_line);
}

sub write_file {
    my ($format, $nb_line) = @_;
    my @letters = ('A'..'Z');
    
    my $file_data = 'data2.txt';
    my $file_data_fh;
    open($file_data_fh, '>', $file_data) or die $!;

    for(my $i = 0; $i < $nb_line; $i++) {
        my $line = ' 'x$format->{LGRECORD};
        foreach my $field (keys (%{$format->{fields}})) {
            my $offset = $format->{fields}->{$field}->{offset};
            my $length = $format->{fields}->{$field}->{length};
            my $data;
            if ($field eq "nomcom") {
                $data = "";
                foreach my $j (1..$length) {
                    my $random_letter = $letters[int rand @letters];
                    $data .= $random_letter;
                }
            } elsif ($field eq "banque") {
                $data = '0'x4 . int(rand(5));
            } elsif ($field eq "code") {
                $data = "01";
            } elsif ($field eq "sscode") {
                $data = "351";
            } else {
                foreach my $j (1..$length) {
                    $data .= int(rand(10));
                }
            }
            substr($line, $offset, $length) = $data;
        }
        $line .= "\n";
        print $file_data_fh $line;
    }

    close($file_data_fh);
}

sub read_format {
    my $file_format = 'format.txt';
    my $file_format_fh;
    open($file_format_fh, '<', $file_format) or die $!;

	# {
    #   LGRECORD: "",
    #   fields: {
    #       field_name: {
	# 		offset: "",
	# 		length: "",
    #       value: ""
	# 	    }
    #   }
	# }
    my $format = {};

    while(my $line = <$file_format_fh>) {
        my @field_data = split(" ", $line);

        if ($line =~ /LGRECORD/) {
			$format->{LGRECORD} = $field_data[1];
		} else {
            if (scalar(@field_data) != 3) {
                die sprintf("length field_data != 3: %s, line: %s", Dumper(\@field_data), $line);
            }
            my $offset = 0 + $field_data[1];
            my $length = 0 + $field_data[2];
            $format->{fields}->{$field_data[0]} = {
                offset => $offset,
                length => $length
            }
        }
    }

    close($file_format_fh);

	return $format;
}
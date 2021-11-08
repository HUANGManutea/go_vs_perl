#!/usr/bin/perl
use strict;
use warnings;
use JSON::XS;
use Data::Dumper qw(Dumper);


main();

sub main {
    my $format = read_format();
    read_file($format);
}

sub read_file {
    my ($format) = @_;
    
    my $file_data = 'data2.txt';
    my $file_data_fh;
    open($file_data_fh, '<', $file_data) or die $!;

    my $result = {};
    
    while(my $line = <$file_data_fh>) {
        foreach my $field (keys (%{$format->{fields}})) {
            my $offset = $format->{fields}->{$field}->{offset};
            my $length = $format->{fields}->{$field}->{length};
            $result->{$field} = substr($line, $offset, $length);
        }
        print(Dumper($result));
    }

    close($file_data_fh);
}

sub read_format {
    my $file_format = 'format.txt';
    my $file_format_fh;
    open($file_format_fh, '<', $file_format) or die $!;

	# {
	# 	field_name: {
	# 		offset: "",
	# 		length: "",
    #       value: ""
	# 	}
	# }
    my $format = {
        LGRECORD => 0,
        fields => {}
    };

    while(my $line = <$file_format_fh>) {
        my @field_data = split(" ", $line);
        if ($line =~ /LGRECORD/) {
			$format->{LGRECORD} = 0+$field_data[1];
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
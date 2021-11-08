#!/usr/bin/perl
use strict;
use warnings;
use JSON::XS;
use Data::Dumper qw(Dumper);
use Coro;
use Coro::Debug;

$|=1;

my $nb_workers = 10;
# queue de lignes du fichier data
my $line_channel = Coro::Channel->new;
# queue des résulats après parsing, input du printer
my $result_channel = Coro::Channel->new;
# queue de fin
my $end_channel = Coro::Channel->new;

# taille d'un tableau de lignes
my $buffer_size = 100;

# format de parsing
my $format;

main();

sub main {
    # ouverture de fichier format
    my $file_format = 'format.txt';
    my $file_format_fh;
    open($file_format_fh, '<', $file_format) or die $!;

    # on construit le format
    $format = read_format($file_format_fh);

    close($file_format_fh);

    # ouverture de fichier data
    my $file_data = 'data2.txt';
    my $file_data_fh;
    open($file_data_fh, '<', $file_data) or die $!;

    # on lance le printer
    async { run_printer() };

    # init des workers (parsers)
    for (1..$nb_workers) {
        async { run_parser() };
    }

    # ps des threads
    Coro::Debug::command "ps";

    # on lit le fichier
    read_file($file_data_fh, $line_channel, $buffer_size);

    # fermeture du fichier data
    close($file_data_fh);

    # on attend la fin du printer (dernier worker à s'arrêter)
    $end_channel->get;
}

sub run_printer {
    # on print les formats finaux
    my $nb_worker_stopped = 0;
    my @received_results;
    while($nb_worker_stopped < $nb_workers) {
        my $result = $result_channel->get;
        if (!defined($result)) {
            $nb_worker_stopped++;
        } else {
            push(@received_results, $result);
        }
    }

    # tous les résultats ont été récupérés, on peut trier
    my @sorted =  sort { $a->{id} <=> $b->{id} } @received_results;

    # pour chaque chunk
    for my $result (@received_results) {
        # pour chaque (ligne formatée) du chunk
        for my $data ($result->{data}) {
            print(Dumper($data));
        } 
    }

    $end_channel->put(1);
}

sub run_parser {
    if (defined($format)) {
        my $stop = 0;
        while (!$stop) {
            my $line_batch = $line_channel->get;
            if (!defined($line_batch)) {
                # il n'y a plus de lignes à traiter, on s'arrête
                $stop = 1;
                # on signale au printer qu'on s'arrête
                $result_channel->put(undef);
            } else {
                # on parse
                my @datas;
                for my $line (@{$line_batch->{lines}}) {
                    my $data = parse_line($format, $line);
                    push(@datas, $data);
                }

                my $id = $line_batch->{id};
                my $result = {
                    id => $id,
                    data => \@datas
                };

                # on met le format dans la sortie
                $result_channel->put($result);
            }
        }
    }
}

sub read_file {
    my ($file_data_fh, $line_channel, $buffer_size) = @_;
    
    my $batch_id = 0;
    my @buffer_line;

    while(my $line = <$file_data_fh>) {
        chomp($line);

        if ($line !~ /^\s*$/) {
            # on met la ligne dans le buffer
            push(@buffer_line, $line);
        }
        
        if (scalar(@buffer_line) == $buffer_size) {
            # copie de l'array car on va reinit
            my @lines = @buffer_line;
            # on associe l'id du batch aux lignes
            my $line_batch = {
                id => $batch_id,
                lines => \@lines
            };
            # on met les lignes dans le channel
            $line_channel->put($line_batch);
            # on reinit
            @buffer_line = ();
            $batch_id+=1;
        }
    }

    # on le refait une dernière fois pour les dernières lignes
    if (scalar(@buffer_line) > 0) {
        # on associe l'id du batch aux lignes
        my $line_batch = {
            id => $batch_id,
            lines => \@buffer_line
        };
        # on met les lignes dans le channel
        $line_channel->put($line_batch);
    }

    # on signale la fin de la lecture aux workers
    for (1..$nb_workers) {
        $line_channel->put(undef);
    }
}

sub parse_line {
    my ($format, $line) = @_;
    # pour chaque champ, récupérer la data en fonction de l'offset et longueur du champ
    my $result = {};
    foreach my $field (keys (%{$format->{fields}})) {
        my $offset = $format->{fields}->{$field}->{offset};
        my $length = $format->{fields}->{$field}->{length};
        $result->{$field} = substr($line, $offset, $length);
    }
    return $result;
}

sub read_format {
    my ($file_format_fh) = @_;

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
            # longueur d'une ligne
			$format->{LGRECORD} = 0+$field_data[1];
		} else {
            # on récupère offset et longueur pour chaque champ
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

	return $format;
}
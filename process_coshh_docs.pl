#!/usr/bin/env perl

### process_coshh_docs.pl

use strict;
use warnings;
use Carp qw{ confess };
use XML::Parser;
use Archive::Zip qw{ :ERROR_CODES };
use JSON;

{
    binmode STDOUT, ':utf8';
    my $parse     = make_table_parser();
    my $processor = make_processor();

    # my $parse = debug_table_parser();
    my $wd = 'word/document.xml';

    my $coshh = [];
    foreach my $file (@ARGV) {
        my $doc_id;
        unless (($doc_id) = $file =~ m{([^/]+)\.docx$}) {
            warn "Skipping non docx file: '$file'\n";
            next;
        }
        my $doc;
        eval {
            my $zip = Archive::Zip->new;
            unless ($zip->read($file) == AZ_OK) {
                die "Error reading '$file'";
            }
            my $word_xml_zip = $zip->memberNamed($wd)
              or die "Failed to fetch word document '$wd' from '$file'";
            my $tables = $parse->($word_xml_zip->contents);
            $doc = { Document_ID => $doc_id };
            $processor->($doc, $tables);
        };
        if ($@) {
            warn "Error processing tables in '$file':\n$@";
            next;
        }
        push @$coshh, $doc;
    }
    print pretty_json_string($coshh);
}

sub make_processor {

    my ($doc, $tbl);
    my @material_fields = qw{
        name_and_cas
        amount_and_form
        signal_word
        hazard
        reportable
    };

    my $looks_like_paragraph_columns = sub {
        my ($mtrl) = @_;

        my $flag = 1;
        foreach my $fld (@material_fields) {
            if (($mtrl->{$fld} || '') !~ /\n\n/) {
                $flag = 0;
                return;
            }
        }
        return $flag;
    };

    my $fix_paragraph_separated_materials = sub {
        my ($mtrl) = @_;

        my @split_cols;
        foreach my $fld (@material_fields) {
            push @split_cols, [ split /\n{2,}/, $mtrl->{$fld} ];
        }

        # Check there is the same number of rows in each column
        my $n_rows = @{ $split_cols[0] };
        for (my $i = 1; $i < @split_cols; $i++) {
            my $col = $split_cols[$i];
            if ($n_rows != @$col) {
                confess "Unbalanced number of rows in materials table:\n", pretty_json_string(\@split_cols);
            }
        }

        # Make new materials from the split columns.
        my @split_materials;
        for (my $i = 0; $i < $n_rows; $i++) {
            my $sm = {};
            @$sm{ @material_fields } = map { $_->[$i] } @split_cols;
            push @split_materials, $sm;
        }
        return @split_materials;
    };

    my @processor_library = (

        sub {
            $doc->{supervisor}        = get_table_match($tbl, 0, 0, qr{Name\s+of\s+supervisor}i, 0, 1);
            $doc->{assessment_number} = get_table_match($tbl, 0, 4, qr{Assessment\s+number}i,    0, 1);
            $doc->{assessor}          = get_table_match($tbl, 2, 0, qr{Assessor}i,               0, 1);
            $doc->{assessment_date}   = get_table_match($tbl, 2, 4, qr{Date\s+of\s+Assessment}i, 0, 1);
        },

        sub {
            $doc->{location_of_work} = get_table_match($tbl, 0, 2, qr{LOCATION\s+OF\s+THE\s+WORK\s+ACTIVITY}i, 0, 2);
        },

        sub {
            my $persons = get_table_match($tbl, 0, 2, qr{PERSONS\s+WHO\s+MAY\s+BE\s+AT\s+RISK}i, 1, -1);
            $persons = scan_table_for_text($tbl, 2, $persons);
            $doc->{persons_at_risk} = $persons;
        },

        sub {
            my $activity = get_table_match($tbl, 0, 2, qr{ACTIVITY\s+ASSESSED}i, 0, 2);
            $activity = scan_table_for_text($tbl, 1, $activity);
            $doc->{activity_assessed} = $activity;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{MATERIALS\s+INVOLVED}i, 0, 0);
            my $materials_list = $doc->{materials} = [];
            for (my $i = 2; $i < @$tbl; $i++) {
                # my ($name_and_cas, $amount_and_form, $signal_word, $hazard, $reportable) = @{ $tbl->[$i] };
                my $mtrl = {};
                @$mtrl{ @material_fields } = @{ $tbl->[$i] };

                if ($looks_like_paragraph_columns->($mtrl)) {
                    push @$materials_list, $fix_paragraph_separated_materials->($mtrl);
                }
                else {
                    push @$materials_list, $mtrl;
                }
            }
        },

        sub {
            get_table_match($tbl, 0, 2, qr{INTENDED\s+USE\s+and\s+JUSTIFICATION}i, 0, 0);
            my $use = scan_table_for_text($tbl, 2);
            $doc->{use_and_justification} = $use;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{RISKS\s+to\s+HEALTH\s+and\s+SAFETY\s+from\s+INTENDED\s+USE}i, 0, 0);
            my $risks = scan_table_for_text($tbl, 2);
            $doc->{risks_to_health_and_safety} = $risks;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{CONCLUSIONS\s+ABOUT\s+RISKS}i, 0, 0);
            my $conclusions = scan_table_for_text($tbl, 2);
            $doc->{risks_conclusions} = $conclusions;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{CONTROL\s+MEASURES}i, 0, 0);
            my $measures = scan_table_for_text($tbl, 2);
            $doc->{control_measures} = $measures;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{INSTRUCTION/TRAINING}i, 0, 0);
            my $train = scan_table_for_text($tbl, 2);
            $doc->{instruction_training} = $train;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{MONITORING}i, 0, 0);
            my ($i, $perf);
            foreach ($i = 2; $i < @$tbl; $i++) {
                my $row = $tbl->[$i];
                last if $row->[0] =~ /Personal\s+exposure/i;
                $perf = nl_append_str($perf, $row->[0]);
            }
            $doc->{monitoring}{performance_control_measures} = $perf;

            my ($exposure, $surveillance);
            for ($i++; $i < @$tbl; $i++) {
                my ($e, $s) = @{ $tbl->[$i] };
                $exposure     = nl_append_str($exposure,     $e) if defined $e;
                $surveillance = nl_append_str($surveillance, $s) if defined $s;
            }
            $doc->{monitoring}{personal_exposure}   = $exposure;
            $doc->{monitoring}{health_surveillance} = $surveillance;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{WASTE\s+DISPOSAL\s+PROCEDURE}i, 0, 0);
            $doc->{waste_disposal_procedure} = scan_table_for_text($tbl, 2);
        },

        sub {
            my $review = get_table_match($tbl, 0, 2, qr{REVIEW}i, 1, -1);
            $review = scan_table_for_text($tbl, 2);
            $doc->{review} = $review;
        },

        sub {
            get_table_match($tbl, 0, 2, qr{EMERGENCY\s+ACTION}i, 0, 0);

            my ($i, $control);
            for ($i = 2; $i < @$tbl; $i++) {
                my $str = $tbl->[$i][0];
                last if $str =~ /TO\s+PROTECT\s+PERSONNEL/i;
                $control = nl_append_str($control, $str);
            }
            $doc->{emergency_action}{to_contol_hazards} = $control;

            my ($site);
            for ($i++; $i < @$tbl; $i++) {
                my $str = $tbl->[$i][0];
                last if $str =~ /TO\s+RENDER\s+SITE\s+OF\s+EMERGENCY\s+SAFE/i;
                $site = nl_append_str($site, $str);
            }
            $doc->{emergency_action}{to_protect_personnel} = $site;

            my ($safe);
            for ($i++; $i < @$tbl; $i++) {
                my $str = $tbl->[$i][0];
                next if $str =~ /^\s*CONTACT\s+PHONE\s*$/i;
                $safe = nl_append_str($safe, $str);
            }
            $doc->{emergency_action}{to_make_site_safe} = $safe;
        },
    );

    return sub {
        $doc       = shift;
        my $tables = shift;

        for (my $i = 0; $i < @processor_library; $i++) {
            my $proc = $processor_library[$i];
            $tbl = $tables->[$i]
              or confess "No table with index = $i in document";
            $proc->();
        }
    }
}

sub scan_table_for_text {
    my ($tbl, $start_row, $str) = @_;

    for (my $i = $start_row || 0; $i < @$tbl; $i++) {
        foreach my $cell (grep { defined } @{ $tbl->[$i] }) {
            $str = nl_append_str($str, $cell);
        }
    }
    return $str;
}

sub nl_append_str {
    my ($str, $oth) = @_;

    return defined($str) ? "$str\n$oth" : $oth;
}

sub get_table_match {
    my ($tbl, $row, $col, $pattern, $row_delta, $col_delta) = @_;

    if ($tbl->[$row][$col] =~ /$pattern/) {
        if (my $txt = $tbl->[$row + $row_delta][$col + $col_delta]) {
            $txt =~ s/(^\s+|\s+$)//;
            return $txt;
        }
        else {
            return;
        }
    }
    else {
        confess "Missing expected cell in tablle:\n", pretty_json_string($tbl);
    }
}

sub make_table_parser {

    my @doc_tables;
    my $table;
    my $row;
    my $string = '';

    my $handle_start = sub {
        my ($expat, $element, %attr) = @_;

        # Table
        if ($element eq 'w:tbl') {
            $table = [];
            push @doc_tables, $table;
        }

        # Table Row
        elsif ($element eq 'w:tr') {
            $row = [];
            push @$table, $row;
        }

        # Table Cell
        elsif ($element eq 'w:tc') {
            $string = '';
        }
    };

    my $handle_end = sub {
        my ($expat, $element, %attr) = @_;

        # Table Row
        if ($element eq 'w:tr') {

            # Remove row if blank
            unless (grep { defined } @$row) {
                pop @$table;
            }
        }

        # Table cell
        elsif ($element eq 'w:tc') {

            # Only retain newlines which separate paragraphs
            $string =~ s/(^\n|\n$)//g;
            if ($string eq '') {
                push @$row, undef;
            }
            else {
                push @$row, $string;
            }
        }

        # Paragraph
        elsif ($element eq 'w:p') {
            $string .= "\n";
        }
    };

    my $handle_char = sub {
        my ($expat, $txt) = @_;

        $string .= $txt;
    };

    my $xmlp = XML::Parser->new(
        Handlers => {
            Start => $handle_start,
            End   => $handle_end,
            Char  => $handle_char,
        }
    );

    my $parser = sub {
        my ($xml) = @_;

        $xmlp->parse($xml);
        my $ret = [@doc_tables];
        @doc_tables = ();
        return $ret;
    };

    return $parser;
}

sub debug_table_parser {

    my $string = '';
    my $level  = 0;

    my $handle_start = sub {
        my ($expat, $element, %attr) = @_;

        $level++;
    };

    my $handle_end = sub {
        my ($expat, $element, %attr) = @_;

        $level--;
        my $spacer = '  ' x $level;
        print qq{$spacer$element "$string"\n};
        $string = '';
    };

    my $handle_char = sub {
        my ($expat, $txt) = @_;

        $string .= $txt;
    };

    return XML::Parser->new(
        Handlers => {
            Start => $handle_start,
            End   => $handle_end,
            Char  => $handle_char,
        }
    );
}

sub pretty_json_string {
    my ($data) = @_;

    my $json = JSON->new;
    $json->convert_blessed(1);
    $json->pretty(1);
    $json->canonical(1);    # Sorts keys alphabetically
    return $json->encode($data);
}

__END__

=head1 NAME - process_coshh_docs.pl

=head1 AUTHOR

James Gilbert B<email> jgrg@sanger.ac.uk


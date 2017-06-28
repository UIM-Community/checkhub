# Require librairies!
use strict;
use warnings;
use lib "D:/apps/Nimsoft/perllib";
use lib "D:/apps/Nimsoft/Perl64/lib/Win32API";
use Nimbus::API;
use Nimbus::CFG;
use Nimbus::PDS;
use Data::Dumper;
use DBIx::Dump;
$Data::Dumper::Indent = 1;
use Term::ANSIColor qw(:constants);
use Win32::Console::ANSI;
use DBI;
use File::Copy;

use bnpp::main;
use bnpp::hub;

#
# Globals variables !
#
my $GBL_STR_ProbeName = "checkhub";
my $GBL_STR_Time_Format = "%.2f";
my $GBL_STR_Ouputdir = "output";
my $GBL_Time_ScriptExecutionTime = time();
my %ExcludedHUBS = ();
my %MonitoredProbes = ();
my $LOGFILE;

unless(open($LOGFILE,">", "checkhub.log")) {
    warn "Unabled to open super log files! \n";
    return;
}

my $DB_File = "checkhub.db";
if(-e $DB_File) {
    print "Unlink database file!\n";
    unlink($DB_File);
}

# ************************************************* #
# DBI
# ************************************************* #
my $DB;
{
    $DB = DBI->connect("dbi:SQLite:dbname=$GBL_STR_ProbeName.db","root","",{
        RaiseError => 1,
        AutoCommit => 0
    }) or die DBI::errstr;

    $DB->do("DROP TABLE IF EXISTS conf");
$DB->do("CREATE TABLE IF NOT EXISTS conf (
    hub VARCHAR(255) NOT NULL,
    probe VARCHAR(100) NOT NULL,
    line TEXT(500) NOT NULL,
    section VARCHAR(255),
    key VARCHAR(50),
    value TEXT(500)
)");
    $DB->commit;
}

#
# Parse and get CFG informations!
#
my $CFG = Nimbus::CFG->new("$GBL_STR_ProbeName.cfg");
my $GBL_NMS_Login       = $CFG->{'setup'}->{'login'};
my $GBL_NMS_Password    = $CFG->{'setup'}->{'password'};
my $GBL_NMS_Domain      = $CFG->{'setup'}->{'domain'};
my $GBL_Viewdirectory   = $CFG->{'setup'}->{'view_directory'};

# Get excluded HUBS !
my @CFG_ExcludedHUBS    = split(",",$CFG->{'setup'}->{'exclude_hubs'});
foreach(@CFG_ExcludedHUBS) {
    $ExcludedHUBS{$_} = 1;
}

# Monitored probes!
my @CFG_Probes = split(",",$CFG->{'setup'}->{'probes'});
foreach(@CFG_Probes) {
    $MonitoredProbes{$_} = 1;
}

#
# Connect to the nimsoft HUB!
#
my $rc = nimLogin("$GBL_NMS_Login","$GBL_NMS_Password");
if(not $rc) {
    print $LOGFILE "Unable to connect to the nimsoft HUB!\n";
    die "Unable to connect to the nimsoft HUB !\n";
}
else {
    print $LOGFILE "Successfully connected to the nimsoft HUB!\n";
}

# Add the SDK!
my $SDK = new bnpp::main("NMS-PROD");
my $SDK_ExecutionTime = $SDK->getDate();
my $SDK_ExecutionPath = "$GBL_STR_Ouputdir/$SDK_ExecutionTime";

# Create directory!
$SDK->createDirectory($SDK_ExecutionPath);
$SDK->createDirectory("$SDK_ExecutionPath/view");
#
# Get all SQL request!
#
my %SQLView = ();
{
    opendir(my $DIR, "$GBL_Viewdirectory") or die $!;
    while(readdir $DIR) {
        if($_ eq "." || $_ eq "..") { # Bug chelou avec la function readdir !
            next;
        }
        if(open(my $conf,'<:encoding(UTF-8)',"$GBL_Viewdirectory/$_")) {
            my $SQL_Request = "";
            while(my $row = <$conf>) {
                $SQL_Request .= $row;
            }
            (my $without_extension = $_) =~ s/\.[^.]+$//;
            $SQLView{$without_extension} = $SQL_Request;
        }
        else {
            warn "failed to open the SQL file $_ \n";
        }
    }
    closedir($DIR);
}

#
# Get all hubs and exclude HUBS from a HASH!
#
my @HUBSArray = $SDK->Get_ArrayHUBS("hub");
my @HUBS = $SDK->excludeHUBS(\@HUBSArray,\%ExcludedHUBS);

my %Final_HUBS = ();

#
# Foreach all nimsoft hubs!
#
for my $HUB (@HUBS) {
    print "---------------------------------------------->\n";
    print "HUB => ".MAGENTA."$HUB->{name}\n".RESET;
    # On crée le repértoire du HUB!
    $SDK->createDirectory("$SDK_ExecutionPath/$HUB->{name}");

    my $RobotPDS = $HUB->Get_Robot($HUB->{robotname});
    if($RobotPDS eq 0) {
        next;
    }
    my $RobotHASH = $RobotPDS->asHash();
    my $addr = $RobotHASH->{"robot"}->{"addr"};

    # On récupère la probe_list du hub!
    my @ProbesArray = ();
    my $count = 3;
    while($count > 0) {
        my $PDS = pdsCreate();
        my ($RC,$NMS_RES) = nimNamedRequest("$addr/controller","probe_list",$PDS,1);
        pdsDelete($PDS);
        if($RC == NIME_OK) {
            my $ProbeCFG = Nimbus::PDS->new($NMS_RES)->asHash();
            foreach my $ProbeName (keys $ProbeCFG) {
                if(exists($MonitoredProbes{$ProbeName})) {
                    my $Iprobe = new bnpp::probe($ProbeName,$ProbeCFG,$addr);
                    push(@ProbesArray,$Iprobe);
                }
            }
            last;
        }
        print RED."\t{TRY $count} Failed to get probe_list!! $RC \n".RESET;
        print $LOGFILE "{TRY $count} Failed to get probe_list for $HUB->{name}, RC Code => $RC \n";
        sleep(1);
        $count--;
    }

    # On récupère les CFG de tout les hubs!
    my $Directory = "$SDK_ExecutionPath/$HUB->{name}";
    my @FailedProbes = ();
    foreach(@ProbesArray) {
        my $RC = $_->getCFG($Directory,$LOGFILE);
        if($RC) {
            $Final_HUBS{$HUB->{name}}{$_->{name}} = "$Directory/$_->{name}.cfg";
            if($_->{name} eq "controller") {
                $Final_HUBS{$HUB->{name}}{"robot"} = "$Directory/robot.cfg";
            }
        }
        else {
            push(@FailedProbes,$_->{name});
        }
    }

    #
    # On génère un rapport .txt des configurations que l'on arrive pas à avoir.
    #
    if(scalar @FailedProbes > 0) {
        my $file_handler;
        unless(open($file_handler,">", "$Directory/fail_get.txt")) {
            warn "Unabled to open failget.txt \n";
            return;
        }
        foreach(@FailedProbes) {
            print $file_handler "$_\n";
        }
        close $file_handler;
    }

}

#
# Insert configuration in the database!
#
print "\n";
foreach my $HUB ( keys %Final_HUBS ) {
    print "---------------------------------------------->\n";
    print "Register configuration for the HUB in the database ".GREEN."$HUB \n".RESET;
    print $LOGFILE "Register configuration for the HUB in the database for HUB $HUB\n";
    foreach my $ProbeName ( keys $Final_HUBS{$HUB} ) {
        print "\t Processs => ".YELLOW."$ProbeName".RESET." configuration.\n";
        print $LOGFILE "Process => $ProbeName configuration.\n";
        my @sectionStoring = ();
        if(open(my $conf, '<:encoding(UTF-8)',$Final_HUBS{$HUB}{$ProbeName})) {
            my $row;
            while($row = <$conf>) {
                $row =~ s/^\s+|\s+$//g;
                my @KV = split("=",$row);
                my $key = $KV[0];
                if( $key =~ /^<\/(.*?)>$/ ) {
                    pop(@sectionStoring);
                }
                elsif ( $key =~ /^<(.*?)>$/){
                    $1 =~ s/\///g;
                    push(@sectionStoring,$1);
                }
                $key =~ s/^\s+|\s+$//g;
                my $value = $KV[1] || "";
                $value =~ s/^\s+|\s+$//g;

                my $finalSection = "";
                foreach(@sectionStoring) {
                    $finalSection .= "/$_";
                }

                my $sth = $DB->prepare("INSERT INTO conf(hub,probe,line,section,key,value) VALUES (?,?,?,?,?,?)");
                $sth->execute($HUB,lc $ProbeName,lc $row,lc $finalSection,$key,$value);
                $sth->finish;
            }
        }
        else {
            warn "Could not open configuration for $ProbeName! \n";
            print $LOGFILE "Could not open configuration for $ProbeName!\n";
        }
    }
}
# On commit le tout dans notre DB!
$DB->commit;
$DB->{AutoCommit} = 1;

#
# Create view!
#
print "\n";
foreach my $fileName (keys %SQLView) {
    print "Starting create view for $fileName!\n";
    print $LOGFILE "Starting create view for $fileName!\n";
    my $sth = $DB->prepare($SQLView{$fileName});
    $sth->execute();
    print GREEN."Finish create view for $fileName \n".RESET;
    print $LOGFILE "Finish create view for $fileName \n";
}

#
# Create CSV File!
#
print "\n";
foreach my $fileName (keys %SQLView) {
    print "Transform $fileName to an CSV\n";
    print $LOGFILE "Transform $fileName to an CSV\n";
    my $SELECT_SQL = "SELECT * FROM $fileName";
    my $excelContent = $DB->prepare($SELECT_SQL);
    $excelContent->execute();
    my $out = DBIx::Dump->new('format' => "excel",'output' => "$SDK_ExecutionPath/view/$fileName.xls",'sth' => $excelContent);
    $out->dump();
    print $LOGFILE "Transform $fileName to an CSV finish!\n";
    print GREEN."Transform $fileName to an CSV finish!\n".RESET;
}

#
# Copy database to the output directory!
#
copy("$GBL_STR_ProbeName.db","$SDK_ExecutionPath/$GBL_STR_ProbeName.db") or warn "SQLite database copy failed: $!";

#
# Final script execution
#
my $GBL_Time_ScriptExecutionTime_End = time();
my $FINAL_TIME = sprintf("$GBL_STR_Time_Format", $GBL_Time_ScriptExecutionTime_End - $GBL_Time_ScriptExecutionTime);
print $LOGFILE "Final execution time $FINAL_TIME second(s) !\n";
print GREEN."\n\nFinal execution time = ".RESET.YELLOW."$FINAL_TIME second(s) !\n".RESET;
close $LOGFILE;

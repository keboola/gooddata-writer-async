<?php
/**
 * Gets csv and convert to GoodData load
 * Arguments:
 * -h names of columns to header separated by comma
 * -d orders of date columns separated by comma
 */
date_default_timezone_set('UTC');

$fh = fopen('php://stdin', 'r');
$stdErr = fopen('php://stderr', 'w');
$stdOut = fopen('php://stdout', 'w');
if (!$fh) {
	fwrite($stdErr, "Error in reading file");
	die(1);
}

$options = getopt('d::t::');

$dateColumns = array();
if (isset($options['d'])) {
	$dateColumns = explode(',', $options['d']);
}
$timeColumns = array();
if (isset($options['t'])) {
	$timeColumns = explode(',', $options['t']);
}

$start = strtotime('1900-01-01 00:00:00');

$rowNumber = 2; // we have to add the header
while ($line = fgets($fh)) {
	$resultLine = '';
	$line = explode('","', $line);
	$line[0] = substr(trim($line[0], "\t\n\r"), 1);
	$lastItemIndex = count($line) - 1;
	$lastItem = rtrim($line[$lastItemIndex], "\t\n\r");
	$line[$lastItemIndex] = substr($lastItem, 0, strlen($lastItem)-1);

	foreach ($line as $i => $column) {
		$resultLine .= '"' . str_replace('\"', '""', $column) . '",';
		if (in_array($i+1, $dateColumns)) {
			// Add date fact (number of days since 1900-01-01 plus one)

			$timestamp = strtotime($column);
			if ($timestamp === false) {
				fwrite($stdErr, sprintf('Error in date column value: "%s" on row %d', $column, $rowNumber));
				die(1);
			}

			if ($start > $timestamp) {
				fwrite($stdErr, sprintf('Error in date column value: "%s" on row %d', $column, $rowNumber));
				die(1);
			}

			$diff = $timestamp - $start;
			$daysDiff = floor($diff/(60*60*24));
			$dateFact = $daysDiff + 1;

			$resultLine .= '"' . $dateFact . '",';

			if (in_array($i+1, $timeColumns)) {
				// Add time fact (number of seconds since midnight)
				$timeFact = $diff - ($daysDiff * 60*60*24);
				$resultLine .= '"' . $timeFact . '",';
				$resultLine .= '"' . $timeFact . '",';
			}
		}
	}

	print rtrim($resultLine, ",") . "\n";
	$rowNumber++;
}
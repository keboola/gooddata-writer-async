<?php
/**
 * Gets csv and convert to GoodData load
 * Arguments:
 * -h names of columns to header separated by comma
 * -d orders of date columns separated by comma
 * -i orders of ignored columns separated by comma
 */

$fh = fopen('php://stdin', 'r');
$stderr = fopen('php://stderr', 'w');
$stdout = fopen('php://stdout', 'w');
if (!$fh) {
	fwrite($stderr, "Error in reading file");
	die(1);
}

$options = getopt('h:i::d::');

$dateColumns = array();
if (isset($options['d'])) {
	$dateColumns = explode(',', $options['d']);
}
$ignoredColumns = array();
if (isset($options['i'])) {
	$ignoredColumns = explode(',', $options['i']);
}

$startDate = new DateTime('1900-01-01');

$rowNumber = 1;
while ($line = fgetcsv($fh)) {
	if ($rowNumber > 1) {
		$resultLine = array();
		foreach ($line as $i => $column) {
			if (!in_array($i+1, $ignoredColumns)) {
				$resultLine[] = $column;
			}
			if (in_array($i+1, $dateColumns)) {
				// Add date fact (number of dates since 1900-01-01 plus one)
				try {
					$columnDate = new DateTime($column);
				} catch(Exception $e) {
					fwrite($stderr, sprintf('Error in date column value: "%s" on row %d', $column, $rowNumber));
					die(1);
				}
				$resultLine[] = (int)$columnDate->diff($startDate)->format('%a') + 1;
			}
		}
		fputcsv($stdout, $resultLine);
	} else {
		fputcsv($stdout, explode(',', $options['h']));
	}
	$rowNumber++;
}

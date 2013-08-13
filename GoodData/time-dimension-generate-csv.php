<?php
$fp = fopen('time-dimension.csv', 'w');
fputcsv($fp, array("second_of_day", "second", "minute_of_day", "minute", "hour", "hour12", "am_pm", "time", "time12"));

for ($sec_of_day = 0; $sec_of_day < 24 * 60 * 60; $sec_of_day++) {
	$sec = floor($sec_of_day % 60);
	$minute_of_day = floor($sec_of_day / 60);
	$minute = floor($minute_of_day % 60);
	$hour = floor($minute_of_day / 60);
	$hour12 = floor(($hour == 12) ? (12) : ($hour % 12));
	$ampm = ($hour < 12) ? ("AM") : ("PM");
	$time = str_pad($hour, 2, 0, STR_PAD_LEFT) . ":" . str_pad($minute, 2, 0, STR_PAD_LEFT) . ":" . str_pad($sec, 2, 0, STR_PAD_LEFT);
	$time12 = str_pad($hour12, 2, 0, STR_PAD_LEFT) . ":" . str_pad($minute, 2, 0, STR_PAD_LEFT) . ":" . str_pad($sec, 2, 0, STR_PAD_LEFT);
	
	fputcsv($fp, array(
		str_pad($sec_of_day, 2, 0, STR_PAD_LEFT),
		str_pad($sec, 2, 0, STR_PAD_LEFT),
		str_pad($minute_of_day, 2, 0, STR_PAD_LEFT),
		str_pad($minute, 2, 0, STR_PAD_LEFT),
		str_pad($hour, 2, 0, STR_PAD_LEFT),
		str_pad($hour12, 2, 0, STR_PAD_LEFT),
		str_pad($ampm, 2, 0, STR_PAD_LEFT),
		str_pad($time, 2, 0, STR_PAD_LEFT),
		str_pad($time12, 2, 0, STR_PAD_LEFT),
	));
}
fclose($fp);
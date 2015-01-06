<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2014-07-01
 */
namespace Keboola\GoodDataWriter\Tests;

use Symfony\Component\Process\Process;
use Keboola\StorageApi\Client as StorageApiClient;

class CsvConversionTest extends \PHPUnit_Framework_TestCase
{
	protected $scriptPath;

	protected function setUp()
	{
		$this->scriptPath = SCRIPTS_PATH . '/convert_csv.php';
	}


	public function testDateFacts()
	{
		$csvHeaders = '"id","name","date","date_dt","date_tm","date_id"';
		$csvRows = '"1","product 1","2013-01-01 00:01:59"
"2","product 2","2013-01-03 11:12:05"
"3","product 3","2012-10-28 23:07:06"
"4","product 3","1351462026"';

		$command = sprintf('echo %s; (echo %s | php %s -d3 -t3)', escapeshellarg($csvHeaders), escapeshellarg($csvRows), escapeshellarg($this->scriptPath));
		$process = new Process($command);
		$process->setTimeout(null);
		$process->run();

		$this->assertTrue($process->isSuccessful(), 'Csv conversion should end with success');
		$this->assertEmpty($process->getErrorOutput(), 'Csv conversion should end without error: ' . $process->getErrorOutput());

		$rows = StorageApiClient::parseCsv($process->getOutput());
		foreach ($rows as $i => $row) {
			$this->assertArrayHasKey('date', $row, 'Row should contain date column');
			$this->assertArrayHasKey('date_dt', $row, 'Row should contain date fact column date_dt');
			$this->assertArrayHasKey('date_tm', $row, 'Row should contain time fact column date_tm');
			$this->assertArrayHasKey('date_id', $row, 'Row should contain time fact column date_id');
			$date = $row['date'];
			$dateFact = $row['date_dt'];
			$timeFact = $row['date_tm'];

			switch ($i) {
				case 0:
					$this->assertEquals('2013-01-01 00:01:59', $date, 'Date column on row one should be equal to 2013-01-01 00:01:59');
					$this->assertEquals(41274, $dateFact, 'Date fact on row one for date 2013-01-01 00:01:59 should be 41274');
					$this->assertEquals(119, $timeFact, 'Time fact on row one for date 2013-01-01 00:01:59 should be 119');
					break;
				case 1:
					$this->assertEquals('2013-01-03 11:12:05', $date, 'Date column on row one should be equal to 2013-01-03 11:12:05');
					$this->assertEquals(41276, $dateFact, 'Date fact on row one for date 2013-01-03 11:12:05 should be 41276');
					$this->assertEquals(40325, $timeFact, 'Time fact on row one for date 2013-01-03 11:12:05 should be 40325');
					break;
				case 2:
					$this->assertEquals('2012-10-28 23:07:06', $date, 'Date column on row one should be equal to 2012-10-28 23:07:06');
					$this->assertEquals(41209, $dateFact, 'Date fact on row one for date 2012-10-28 23:07:06 should be 41209');
					$this->assertEquals(83226, $timeFact, 'Time fact on row one for date 2012-10-28 23:07:06 should be 83226');
					break;
			}
		}
	}

	public function testEscaping()
	{
		$csvPath = __DIR__ . '/data/escaping.csv';
		$this->assertFileExists($csvPath, 'Csv file for scaping test should exist');

		$command = sprintf('cat %s | php %s', escapeshellarg($csvPath), escapeshellarg($this->scriptPath));
		$process = new Process($command);
		$process->setTimeout(null);
		$process->run();

		$this->assertTrue($process->isSuccessful(), 'Csv conversion should end with success');
		$this->assertEmpty($process->getErrorOutput(), 'Csv conversion should end without error: ' . $process->getErrorOutput());

		$rows = StorageApiClient::parseCsv($process->getOutput());
		foreach ($rows as $i => $row) {
			$this->assertCount(2, $row, 'Row ' . $i . ' should contain two columns');
			$this->assertArrayHasKey('col1', $row, 'Row should contain col1 column');
			$this->assertArrayHasKey('col2', $row, 'Row should contain col2 column');
		}
	}

}

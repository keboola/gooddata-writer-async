<?php
/**
 * GoodData API class
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2012-01-17
 *
 */

namespace Keboola\GoodDataWriter\GoodData;

use Monolog\Logger;
use Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;

class CLToolApi
{

	/**
	 * Number of retries for one API call
	 */
	const RETRIES_COUNT = 5;

	/**
	 * Back off time before retrying API call
	 */
	const BACKOFF_INTERVAL = 60;


	/**
	 * @var string GD username
	 */
	private $_username;
	/**
	 * @var string GD password
	 */
	private $_password;
	/**
	 * @var string GoodData backend url
	 */
	private $_backendUrl = 'secure.gooddata.com';
	/**
	 * @var Logger
	 */
	private $_log;



	/**
	 * @var string
	 */
	public $tmpDir;
	/**
	 * @var string
	 */
	public $clToolPath;
	public $rootPath;
	/**
	 * @var \Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader
	 */
	public $s3uploader;
	public $jobId;

	/**
	 * @var
	 */
	public $debugLogUrl;
	public $output;



	/**
	 * @param Logger $log
	 */
	public function __construct(Logger $log)
	{
		$this->_log = $log;
	}

	public function setCredentials($username, $password)
	{
		$this->_username = $username;
		$this->_password = $password;
	}

	/**
	 * Change default backend url
	 * @param $backendUrl
	 */
	public function setBackendUrl($backendUrl)
	{
		$this->_backendUrl = $backendUrl;
	}

	/**
	 * Common wrapper for GD CLI commands
	 *
	 * @param $args
	 *
	 * @param $args
	 * @throws CLToolApiErrorException
	 * @throws \Exception
	 * @throws \Exception
	 */
	public function call($args)
	{
		// Prepare directory for logs
		$prevCwd = getcwd();
		$workingDirectory = $this->tmpDir . '/' . uniqid('gooddata-export', TRUE);
		if (!mkdir($workingDirectory)) {
			throw new \Exception('GoodDataExport: cannot create dir: ' . $workingDirectory);
		}

		if (!chdir($workingDirectory)) {
			throw new \Exception('GoodDataExport: cannot change dir: ' . $workingDirectory);
		}

		$cleanUp = function() use($workingDirectory, $prevCwd) {
			system('rm -rf ' . escapeshellarg($workingDirectory));
			chdir($prevCwd);
		};

		// Assemble CL tool command
		$command = escapeshellarg('/opt/ebs-disk/GD/cli/bin/gdi.sh')
			. ' -u ' . escapeshellarg($this->_username)
			. ' -p ' . escapeshellarg($this->_password)
			. ' -h ' . escapeshellarg($this->_backendUrl)
			. ' --timezone=GMT'
			. ' -e ' . escapeshellarg($args);
		$outputFile = $workingDirectory . '/output-' . date('Ymd-His') . '-' . uniqid() . '.txt';
		file_put_contents($outputFile . '.1', $args . "\n\n");

		$backoffInterval = self::BACKOFF_INTERVAL;
		for ($i = 0; $i < self::RETRIES_COUNT; $i++) {
			exec($command . ' 2>&1 > ' . escapeshellarg($outputFile . '.2'));
			exec('cat ' . escapeshellarg($outputFile . '.1') . ' ' . escapeshellarg($outputFile . '.2') .' > ' . escapeshellarg($outputFile));
			exec('rm ' . escapeshellarg($outputFile . '.1'));
			exec('rm ' . escapeshellarg($outputFile . '.2'));

			// Test output for server error
			$apiErrorTest = "egrep '503 Service Unavailable' " . escapeshellarg($outputFile);

			if (!shell_exec($apiErrorTest)) {

				if (file_exists($workingDirectory . '/debug.log')) {
					exec(sprintf('cat %s %s > %s ', escapeshellarg($outputFile), escapeshellarg($workingDirectory . '/debug.log'), escapeshellarg($outputFile . '.D')));
					exec(sprintf('rm %s', escapeshellarg($outputFile)));
					exec(sprintf('mv %s %s ', escapeshellarg($outputFile . '.D'), escapeshellarg($outputFile)));
				}

				$this->debugLogUrl = $this->s3uploader->uploadFile($outputFile);

				// Test output for runtime error
				if (shell_exec("egrep 'com.gooddata.exception.HttpMethodException: 401 Unauthorized' " . escapeshellarg($outputFile))) {
					// Backoff and try again
				} elseif (shell_exec(sprintf("cat %s | grep -v 'SocketException' | egrep 'ERROR|Exception'", escapeshellarg($outputFile)))) {
					throw new CLToolApiErrorException('CL Tool Error, see debug log for details: ' . $this->debugLogUrl);
				}

				$cleanUp();
				return;
			} else {
				// Wait indefinitely
				$i--;
				$backoffInterval = 10 * 60;
			}

			sleep($backoffInterval * ($i + 1));
		}

		$cleanUp();
		throw new \Exception('GoodData Service Unavailable', 400);
	}


	/**
	 * Set of commands which create a date
	 * @param string $pid
	 * @param string $name
	 * @param bool $includeTime
	 * @throws CLToolApiErrorException
	 * @return string|bool
	 */
	public function createDate($pid, $name, $includeTime = FALSE)
	{
		$maqlFile = $this->tmpDir . '/' . $pid . '-' . date('Ymd-His') . '-createDate-' . $name . '.maql';

		$command  = 'OpenProject(id="' . $pid . '");';
		$command .= 'UseDateDimension(name="' . $name . '", includeTime="' . ($includeTime ? 'true' : 'false') . '");';
		$command .= 'GenerateMaql(maqlFile="' . $maqlFile . '");';
		$command .= 'ExecuteMaql(maqlFile="' . $maqlFile . '");';
		$command .= 'TransferData();';

		$this->output  = '*** CL Tool Command ***' . PHP_EOL . $command . PHP_EOL . PHP_EOL;

		$this->call($command);

		if (file_exists($maqlFile)) {
			$this->output .= '*** Generated MAQL ***' . PHP_EOL . file_get_contents($maqlFile) . PHP_EOL . PHP_EOL;
			unlink($maqlFile);
		} else {
			throw new CLToolApiErrorException();
		}
	}


	/**
	 * Set of commands which create a dataset
	 * @param string $pid
	 * @param string $xmlFile
	 * @throws CLToolApiErrorException
	 * @throws \Exception
	 */
	public function createDataset($pid, $xmlFile)
	{
		if (file_exists($xmlFile)) {
			libxml_use_internal_errors(TRUE);
			$sxml = simplexml_load_file($xmlFile);
			if ($sxml) {
				$datasetName = (string)$sxml->name;

				$maqlFile = $this->tmpDir . '/' . $pid . '-' . date('Ymd-His') . '-createDataset-' . $datasetName . '.maql';

				$csvFile = $this->tmpDir . '/dummy.csv';
				if (!file_exists($csvFile)) touch($csvFile);

				$command  = 'OpenProject(id="' . $pid . '");';
				$command .= 'UseCsv(csvDataFile="' . $csvFile . '", hasHeader="true", configFile="' . $xmlFile . '");';
				$command .= 'GenerateMaql(maqlFile="' . $maqlFile . '");';
				$command .= 'ExecuteMaql(maqlFile="' . $maqlFile . '");';

				$this->output  = '*** CL Tool Command ***' . PHP_EOL . $command . PHP_EOL . PHP_EOL;

				$this->call($command);

				if (file_exists($maqlFile)) {
					$this->output .= '*** Generated MAQL ***' . PHP_EOL . file_get_contents($maqlFile) . PHP_EOL . PHP_EOL;
					unlink($maqlFile);
				} else {
					throw new CLToolApiErrorException();
				}
			} else {
				$errors = '';
				foreach (libxml_get_errors() as $error) {
					$errors .= $error->message;
				}
				$this->output = '*** Error in XML ***' . PHP_EOL . $errors;
				throw new CLToolApiErrorException();
			}
		} else {
			throw new \Exception('XML file does not exist: ' . $xmlFile);
		}
	}

	/**
	 * Set of commands which create a dataset
	 * @param string $pid
	 * @param string $xmlFile
	 * @param bool $updateAll
	 * @throws CLToolApiErrorException
	 * @throws \Exception
	 * @return string|bool
	 */
	public function updateDataset($pid, $xmlFile, $updateAll=FALSE)
	{
		if (file_exists($xmlFile)) {
			libxml_use_internal_errors(TRUE);
			$sxml = simplexml_load_file($xmlFile);
			if ($sxml) {
				$datasetName = (string)$sxml->name;
				$maqlFile = $this->tmpDir . '/' . $pid . '-' . date('Ymd-His') . '-updateDataset-' . $datasetName . '.maql';
				$csvFile = $this->tmpDir . '/dummy.csv';
				if (!file_exists($csvFile)) touch($csvFile);

				$command  = 'OpenProject(id="' . $pid . '");';
				$command .= 'UseCsv(csvDataFile="' . $csvFile . '", hasHeader="true", configFile="' . $xmlFile . '");';
				$command .= 'GenerateUpdateMaql(maqlFile="' . $maqlFile . '"';
				if ($updateAll) {
					$command .= ' updateAll="true"';
				}
				$command .= ');';


				$this->output  = '*** CL Tool Command ***' . PHP_EOL . $command . PHP_EOL . PHP_EOL;

				$this->call($command);

				if (file_exists($maqlFile)) {
					$this->output .= '*** Generated MAQL ***' . PHP_EOL . file_get_contents($maqlFile) . PHP_EOL . PHP_EOL;

					$command = 'OpenProject(id="' . $pid . '"); ExecuteMaql(maqlFile="' . $maqlFile . '");';

					$this->output .= '*** CL Tool Command ***' . PHP_EOL . $command . PHP_EOL . PHP_EOL;

					$this->call($command);

					unlink($maqlFile);
				}
			} else {
				$errors = '';
				foreach (libxml_get_errors() as $error) {
					$errors .= $error->message;
				}
				$this->output = '*** Error in XML ***' . PHP_EOL . $errors;
				throw new CLToolApiErrorException();
			}
		} else {
			throw new \Exception('XML file does not exist: ' . $xmlFile);
		}
	}

	/**
	 * Set of commands which loads data to data set
	 * @param string $pid
	 * @param string $xmlFile
	 * @param string $csvFile
	 * @param bool $incremental
	 * @throws CLToolApiErrorException
	 * @throws \Exception
	 * @return string|bool
	 */
	public function loadData($pid, $xmlFile, $csvFile, $incremental = FALSE)
	{
		if (file_exists($xmlFile)) {
			if (file_exists($csvFile)) {
				$command  = 'OpenProject(id="' . $pid . '");';
				$command .= 'UseCsv(csvDataFile="' . $csvFile . '", hasHeader="true", configFile="' . $xmlFile . '");';
				$command .= 'TransferData(incremental="' . ($incremental ? 'true' : 'false') . '", waitForFinish="true");';

				$this->output  = '*** CL Tool Command ***' . PHP_EOL . $command . PHP_EOL . PHP_EOL;

				$this->call($command);

				return array('gdWriteBytes' => filesize($csvFile));
			} else {
				throw new \Exception('CSV file does not exist: ' . $csvFile);
			}
		} else {
			throw new \Exception('XML file does not exist: ' . $xmlFile);
		}
	}

	/**
	 * @param $pid
	 * @return string|bool
	 */
	public function executeReports($pid)
	{
		$maqlFile = $this->tmpDir . '/temp-' . date('Ymd-His') . '-' . uniqid() . '.maql';

		$command  = 'OpenProject(id="' . $pid . '");';
		$command .= 'GetReports(fileName="' . $maqlFile . '");';

		$this->call($command);

		if (file_exists($maqlFile) && filesize($maqlFile)) {
			$command  = 'OpenProject(id="' . $pid . '");';
			$command .= 'ExecuteReports(fileName="' . $maqlFile . '");';
			$this->call($command);

			unlink($maqlFile);
		}
	}


}

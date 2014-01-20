<?php
/**
 * GoodData API class
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2012-01-17
 *
 */

namespace Keboola\GoodDataWriter\GoodData;

use Monolog\Logger;
use Keboola\GoodDataWriter\Service\S3Client;
use Symfony\Component\Process\Process;

class CLToolApiErrorException extends \Exception
{

}

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
	private $_backendUrl = 'na1.secure.gooddata.com';
	/**
	 * @var Logger
	 */
	private $_log;
    private $_clPath;


	/**
	 * @var string
	 */
	public $tmpDir;
	/**
	 * @var S3Client
	 */
	public $s3client;
	public $s3Dir;
	public $jobId;

	/**
	 * @var
	 */
	public $debugLogUrl;
	public $output;


    /**
     * @param $clPath
     * @param Logger $log
     */
	public function __construct(Logger $log, $clPath = null)
	{
        $this->_log = $log;
        if ($clPath) {
            $this->_clPath = $clPath;
        }
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
		if (!chdir($this->tmpDir)) {
			throw new \Exception('GoodDataExport: cannot change dir: ' . $this->tmpDir);
		}

        $clPath = $this->_clPath ? $this->_clPath : '/opt/ebs-disk/GD/cli/bin/gdi.sh';
		// Assemble CL tool command
		$command = escapeshellarg($clPath)
			. ' -u ' . escapeshellarg($this->_username)
			. ' -p ' . escapeshellarg($this->_password)
			. ' -h ' . escapeshellarg($this->_backendUrl)
			. ' --timezone=GMT'
			. ' -e ' . escapeshellarg($args);
		$outputFile = $this->tmpDir . '/cl-output.txt';
		file_put_contents($outputFile . '.1', $args . "\n\n");

		$backOffInterval = 10 * 60;
		for ($i = 0; $i < self::RETRIES_COUNT; $i++) {
            $escapedFile = escapeshellarg($outputFile);
            $escapedFile1 = escapeshellarg($outputFile . '.1');
            $escapedFile2 = escapeshellarg($outputFile . '.2');

            $runCommand  = sprintf('%s 2>&1 > %s;', $command, $escapedFile2);
            $runCommand .= sprintf('cat %s %s > %s;', $escapedFile1, $escapedFile2, $escapedFile);
            $runCommand .= sprintf('rm %s;', $escapedFile1);
            $runCommand .= sprintf('rm %s;', $escapedFile2);
            $process = new Process($runCommand);
            $process->setTimeout(null);
            $process->run();
            if (!$process->isSuccessful()) {
                $message = $process->getErrorOutput() ? $process->getErrorOutput() : 'No output';
                throw new \Exception('CL tool Error: ' . $message);
            }

			// Test output for server error
			if (!shell_exec("egrep '503 Service Unavailable' " . $escapedFile)) {

				if (file_exists($this->tmpDir . '/debug.log')) {

                    $escapedFileD = escapeshellarg($outputFile . '.D');
                    $runCommand  = sprintf('cat %s %s > %s;', $escapedFile, escapeshellarg($this->tmpDir . '/debug.log'), $escapedFileD);
                    $runCommand .= sprintf('rm %s;', $escapedFile);
                    $runCommand .= sprintf('mv %s %s;', $escapedFileD, $escapedFile);
                    $process = new Process($runCommand);
                    $process->setTimeout(null);
                    $process->run();
                    if (!$process->isSuccessful()) {
                        $message = $process->getErrorOutput() ? $process->getErrorOutput() : 'No output';
                        throw new \Exception('CL tool Error: ' . $message);
                    }
				}

				$this->debugLogUrl = $this->s3client->uploadFile($outputFile, 'text/plain', $this->s3Dir . '/cl-output.txt');

				// Test output for runtime error
				if (shell_exec("egrep 'com.gooddata.exception.HttpMethodException: 401 Unauthorized' " . $escapedFile)) {
					// Backoff and try again
				} elseif (shell_exec(sprintf("cat %s | grep -v 'SocketException' | egrep 'ERROR|Exception'", $escapedFile))) {
					throw new CLToolApiErrorException('CL Tool Error, see debug log for details');
				}

				return;
			} else {
				// Wait indefinitely
				$i--;
			}

			sleep($backOffInterval * ($i + 1));
		}

		throw new \Exception('GoodData Service Unavailable', 400);
	}


	/**
	 * Set of commands which create a dataset
	 * @param string $pid
	 * @param string $xmlFile
	 * @return array
	 * @throws CLToolApiErrorException
	 * @throws \Exception
	 */
	public function createDataSetMaql($pid, $xmlFile)
	{
		$this->output = array();
		if (file_exists($xmlFile)) {

			libxml_use_internal_errors(TRUE);
			$sXml = simplexml_load_file($xmlFile);
			if ($sXml) {
				$dataSetName = (string)$sXml->name;

				$maqlFile = $this->tmpDir . '/createDataset-' . $dataSetName . '.maql';

				$csvFile = $this->tmpDir . '/dummy.csv';
				if (!file_exists($csvFile)) touch($csvFile);

				$command  = 'OpenProject(id="' . $pid . '"); ';
				$command .= 'UseCsv(csvDataFile="' . $csvFile . '", hasHeader="true", configFile="' . $xmlFile . '"); ';
				$command .= 'GenerateMaql(maqlFile="' . $maqlFile . '"); ';

				$this->output['command'] = $command;
				$this->call($command);

				if (file_exists($maqlFile)) {
					$maql = file_get_contents($maqlFile);
					$this->output['maql'] = $maql;
					unlink($maqlFile);

					return $maql;
				} else {
					throw new CLToolApiErrorException('Maql file was not created.');
				}
			} else {
				$this->output['errors'] = libxml_get_errors();
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
	public function updateDataSetMaql($pid, $xmlFile, $updateAll=FALSE)
	{
		$this->output = array();
		if (file_exists($xmlFile)) {

			libxml_use_internal_errors(TRUE);
			$sXml = simplexml_load_file($xmlFile);
			if ($sXml) {
				$dataSetName = (string)$sXml->name;
				$maqlFile = $this->tmpDir . '/updateDataset-' . $dataSetName . '.maql';
				$csvFile = $this->tmpDir . '/dummy.csv';
				if (!file_exists($csvFile)) touch($csvFile);

				$command  = 'OpenProject(id="' . $pid . '"); ';
				$command .= 'UseCsv(csvDataFile="' . $csvFile . '", hasHeader="true", configFile="' . $xmlFile . '"); ';
				$command .= 'GenerateUpdateMaql(maqlFile="' . $maqlFile . '" rebuildLabels="false"';
				if ($updateAll) {
					$command .= ' updateAll="true"';
				}
				$command .= '); ';


				$this->output['command'] = $command;
				$this->call($command);

				if (file_exists($maqlFile)) {
					$maql = file_get_contents($maqlFile);
					$this->output['maql'] = $maql;
					unlink($maqlFile);

					return $maql;
				} else {
					return false;
				}
			} else {
				$this->output['errors'] = libxml_get_errors();
				throw new CLToolApiErrorException();
			}
		} else {
			throw new \Exception('XML file does not exist: ' . $xmlFile);
		}
	}

}

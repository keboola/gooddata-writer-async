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
use Keboola\GoodDataWriter\Exception\ClientException;

class CLToolApiErrorException extends ClientException
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

        $clPath = 'sh '. ($this->_clPath ? $this->_clPath : '/opt/ebs-disk/GD/cli/bin/gdi.sh');
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
			if (!$process->isSuccessful() || $process->getErrorOutput() || substr($process->getOutput(), 0, 9) == 'Exception') {
				$message = null;
				if ($process->getOutput()) {
					$message .= $process->getOutput() . "\n";
				}
				if ($process->getErrorOutput()) {
					$message .= $process->getErrorOutput();
				}
				if (!$message) {
					$message = 'No output';
				}
                throw new CLToolApiErrorException($message);
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
	 */
	public function createDataSetMaql($pid, $xml, $dataSetName)
	{
		$xmlFilePath = $this->tmpDir . '/model.xml';
		file_put_contents($xmlFilePath, $xml);

		$this->output = array();
		if (file_exists($xmlFilePath)) {
			$maqlFile = $this->tmpDir . '/createDataset-' . $dataSetName . '.maql';

			$csvFile = $this->tmpDir . '/dummy.csv';
			if (!file_exists($csvFile)) touch($csvFile);

			$command  = 'OpenProject(id="' . $pid . '"); ';
			$command .= 'UseCsv(csvDataFile="' . $csvFile . '", hasHeader="true", configFile="' . $xmlFilePath . '"); ';
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
			throw new \Exception('XML file does not exist: ' . $xmlFilePath);
		}
	}

	/**
	 * Set of commands which updates a dataset
	 */
	public function updateDataSetMaql($pid, $xml, $updateAll=FALSE, $dataSetName)
	{
		$xmlFilePath = $this->tmpDir . '/model.xml';
		file_put_contents($xmlFilePath, $xml);

		$this->output = array();
		if (file_exists($xmlFilePath)) {

			$maqlFile = $this->tmpDir . '/updateDataset-' . $dataSetName . '.maql';
			$csvFile = $this->tmpDir . '/dummy.csv';
			if (!file_exists($csvFile)) touch($csvFile);

			$command  = 'OpenProject(id="' . $pid . '"); ';
			$command .= 'UseCsv(csvDataFile="' . $csvFile . '", hasHeader="true", configFile="' . $xmlFilePath . '"); ';
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
			throw new \Exception('XML file does not exist: ' . $xmlFilePath);
		}
	}


	/**
	 *
	 */
	public static function getXml($definition)
	{
		$xml = new \DOMDocument();
		$schema = $xml->createElement('schema');
		$name = $xml->createElement('name', $definition['name']);
		$schema->appendChild($name);
		$columns = $xml->createElement('columns');

		foreach ($definition['columns'] as $columnDefinition) {

			$column = $xml->createElement('column');
			$column->appendChild($xml->createElement('name', $columnDefinition['name']));
			$column->appendChild($xml->createElement('title', $columnDefinition['title']));
			$column->appendChild($xml->createElement('ldmType', $columnDefinition['type']));
			if ($columnDefinition['type'] != 'FACT') {
				$column->appendChild($xml->createElement('folder', $definition['name']));
			}

			if (!empty($columnDefinition['dataType'])) {
				$column->appendChild($xml->createElement('dataType', $columnDefinition['dataType']));
			}

			if (!empty($columnDefinition['sortLabel'])) {
				$column->appendChild($xml->createElement('sortLabel', $columnDefinition['sortLabel']));
				$column->appendChild($xml->createElement('sortOrder', !empty($columnDefinition['sortOrder'])
					? $columnDefinition['sortOrder'] : 'ASC'));
			}

			if (!empty($columnDefinition['reference'])) {
				$column->appendChild($xml->createElement('reference', $columnDefinition['reference']));
			}

			if (!empty($columnDefinition['schemaReference'])) {
				$column->appendChild($xml->createElement('schemaReference', $columnDefinition['schemaReference']));
			}

			if ($columnDefinition['type'] == 'DATE') {
				$column->appendChild($xml->createElement('format', $columnDefinition['format']));
				$column->appendChild($xml->createElement('datetime', $columnDefinition['includeTime'] ? 'true' : 'false'));
			}

			$columns->appendChild($column);
		}

		$schema->appendChild($columns);
		$xml->appendChild($schema);

		return $xml->saveXML();
	}

}

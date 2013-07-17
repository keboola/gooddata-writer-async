<?php
/**
 *
 * User: Martin Halamíček
 * Date: 25.6.12
 * Time: 16:08
 *
 */

namespace Keboola\GoodDataWriter\Writer;


class ProcessException extends \Exception
{

}

class Process
{
	const FD_READ = 1;
	const FD_WRITE = 0;
	const FD_ERR = 2;

	public static  function exec($cmd)
	{
		$output = '';
		static::run($cmd, function($buffer) use(&$output) {
			$output .= $buffer;
		});
		return $output;
	}

	public static  function  runPassThru($cmd)
	{
		$outputSize = 0;
		ob_implicit_flush(true);
		ob_end_flush();

		static::run($cmd, function($buffer) use(&$outputSize) {
			echo $buffer;
			$outputSize += strlen($buffer);
		});

		return $outputSize;
	}

	/**
	 * @static
	 * @param $cmd
	 * @throws ProcessException
	 */
	public static function run($cmd, $onBuffer)
	{

		if (!is_callable($onBuffer)) {
			throw new ProcessException('Callback is not callable');
		}

		$descriptorspec = array(
			0 => array("pipe", "r"),
			1 => array("pipe", "w"),
			2 => array("pipe", "w")
		);

		$ptr = proc_open($cmd, $descriptorspec, $pipes, NULL, $_ENV);
		if (!is_resource($ptr)) {
			throw new ProcessException('Cannot run command');
		}

		$errbuf = '';
		$buffer = '';
		while (($buffer = fgets($pipes[self::FD_READ])) != NULL
			|| ($errbuf = fgets($pipes[self::FD_ERR])) != NULL) {

			if (strlen($buffer)) {
				call_user_func($onBuffer, $buffer);
			}
			if (strlen($errbuf)) {
				throw new ProcessException($errbuf);
			}
		}

		foreach ($pipes as $pipe) {
			fclose($pipe);
		}

		/* Get the expected *exit* code to return the value */
		$pstatus = proc_get_status($ptr);
		if (!strlen($pstatus["exitcode"]) || $pstatus["running"]) {
			/* we can trust the retval of proc_close() */
			if ($pstatus["running"])
				proc_terminate($ptr);
			$ret = proc_close($ptr);
		} else {
			proc_close($ptr);
		}
	}

}
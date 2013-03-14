<?php

namespace Keboola\GoodDataWriterBundle\Exception;

use Symfony\Component\HttpKernel\Exception\HttpException;

class WrongParametersException extends HttpException
{
	public function __construct($message)
	{
		parent::__construct(400, $message);
	}
}
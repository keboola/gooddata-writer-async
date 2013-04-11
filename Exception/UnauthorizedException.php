<?php

namespace Keboola\GoodDataWriter\Exception;

use Symfony\Component\HttpKernel\Exception\HttpException;

class UnauthorizedException extends HttpException
{
	public function __construct($message)
	{
		parent::__construct(400, $message);
	}
}
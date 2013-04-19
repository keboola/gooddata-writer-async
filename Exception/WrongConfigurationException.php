<?php

namespace Keboola\GoodDataWriter\Exception;

use Syrup\ComponentBundle\Exception\SyrupComponentException;

class WrongConfigurationException extends SyrupComponentException
{
	public function __construct($message)
	{
		parent::__construct(400, $message);
	}
}
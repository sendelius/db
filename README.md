# Sendelius Db

<p>
<a href="https://packagist.org/packages/sendelius/db"><img src="https://img.shields.io/packagist/dt/sendelius/db" alt="Total Downloads"></a>
<a href="https://packagist.org/packages/sendelius/db"><img src="https://img.shields.io/packagist/v/sendelius/db" alt="Latest Stable Version"></a>
<a href="https://packagist.org/packages/sendelius/db"><img src="https://img.shields.io/packagist/l/sendelius/db" alt="License"></a>
</p>

Лёгкая и удобная библиотека для работы с базой данных на PHP.

## Установка

```
composer require sendelius/db
```

## Пример использования
```php
require 'vendor/autoload.php';

use \Sendelius\Db\Db;

$db = new Db();
$db->connect('dbName','dbHost','dbUser','dbPassword');
```

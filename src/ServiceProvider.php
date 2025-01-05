<?php

namespace vkleo\medoo;

use support\ServiceProvider as supportServiceProvider;

class ServiceProvider extends supportServiceProvider
{

    public function register()
    {
        $this->app->singleton('myMedoo', function ($app) {
            return MedooChainQuery::getInstance();
        });

        $this->app->alias('myMedoo', MedooChainQuery::class);
    }

    public function boot()
    {
        // 启动时需要执行的代码
    }
}
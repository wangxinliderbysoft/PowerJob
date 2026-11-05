create sequence native start 1 increment 1;

    create table compat_app_info (
       id int8 not null,
        app_name varchar(255),
        creator int8,
        current_server varchar(255),
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        modifier int8,
        namespace_id int8,
        password varchar(255),
        tags varchar(255),
        title varchar(255),
        primary key (id)
    );

    create table compat_container_info (
       id int8 not null,
        app_id int8,
        container_name varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        last_deploy_time timestamp,
        source_info varchar(255),
        source_type int4,
        status int4,
        version varchar(255),
        primary key (id)
    );

    create table compat_instance_info (
       id int8 not null,
        actual_trigger_time int8,
        app_id int8,
        expected_trigger_time int8,
        extend_value varchar(255),
        finished_time int8,
        gmt_create timestamp,
        gmt_modified timestamp,
        instance_id int8,
        instance_params text,
        job_id int8,
        job_params text,
        last_report_time int8,
        meta varchar(255),
        outer_key varchar(255),
        result text,
        running_times int8,
        status int4,
        task_tracker_address varchar(255),
        type int4,
        wf_instance_id int8,
        primary key (id)
    );

    create table compat_job_info (
       id int8 not null,
        advanced_runtime_config varchar(255),
        alarm_config varchar(255),
        app_id int8,
        concurrency int4,
        designated_workers varchar(255),
        dispatch_strategy int4,
        dispatch_strategy_config varchar(255),
        execute_type int4,
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        instance_retry_num int4,
        instance_time_limit int8,
        job_description varchar(255),
        job_name varchar(255),
        job_params text,
        lifecycle varchar(255),
        log_config varchar(255),
        max_instance_num int4,
        max_worker_count int4,
        min_cpu_cores float8 not null,
        min_disk_space float8 not null,
        min_memory_space float8 not null,
        next_trigger_time int8,
        notify_user_ids varchar(255),
        processor_info varchar(255),
        processor_type int4,
        status int4,
        tag varchar(255),
        task_retry_num int4,
        time_expression varchar(255),
        time_expression_type int4,
        primary key (id)
    );

    create table compat_namespace (
       id int8 not null,
        code varchar(255),
        creator int8,
        dept varchar(255),
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        modifier int8,
        name varchar(255),
        status int4,
        tags varchar(255),
        token varchar(255),
        primary key (id)
    );

    create table compat_oms_lock (
       id int8 not null,
        gmt_create timestamp,
        gmt_modified timestamp,
        lock_name varchar(255),
        max_lock_time int8,
        ownerip varchar(255),
        primary key (id)
    );

    create table compat_pwjb_user_info (
       id int8 not null,
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        password varchar(255),
        username varchar(255),
        primary key (id)
    );

    create table compat_server_info (
       id int8 not null,
        gmt_create timestamp,
        gmt_modified timestamp,
        ip varchar(255),
        primary key (id)
    );

    create table compat_sundry (
       id int8 not null,
        content varchar(255),
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        pkey varchar(255),
        skey varchar(255),
        primary key (id)
    );

    create table compat_user_info (
       id int8 not null,
        account_type varchar(255),
        email varchar(255),
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        nick varchar(255),
        origin_username varchar(255),
        password varchar(255),
        phone varchar(255),
        status int4,
        token_login_verify_info varchar(255),
        username varchar(255),
        web_hook varchar(255),
        primary key (id)
    );

    create table compat_user_role (
       id int8 not null,
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        role int4,
        scope int4,
        target int8,
        user_id int8,
        primary key (id)
    );

    create table compat_workflow_info (
       id int8 not null,
        app_id int8,
        extra varchar(255),
        gmt_create timestamp,
        gmt_modified timestamp,
        lifecycle varchar(255),
        max_wf_instance_num int4,
        next_trigger_time int8,
        notify_user_ids varchar(255),
        pedag text,
        status int4,
        time_expression varchar(255),
        time_expression_type int4,
        wf_description varchar(255),
        wf_name varchar(255),
        primary key (id)
    );

    create table compat_workflow_instance_info (
       id int8 not null,
        actual_trigger_time int8,
        app_id int8,
        dag text,
        expected_trigger_time int8,
        finished_time int8,
        gmt_create timestamp,
        gmt_modified timestamp,
        parent_wf_instance_id int8,
        result text,
        status int4,
        wf_context text,
        wf_init_params text,
        wf_instance_id int8,
        workflow_id int8,
        primary key (id)
    );

    create table compat_workflow_node_info (
       id int8 not null,
        app_id int8 not null,
        enable boolean not null,
        extra text,
        gmt_create timestamp not null,
        gmt_modified timestamp not null,
        job_id int8,
        node_name varchar(255),
        node_params text,
        skip_when_failed boolean not null,
        type int4,
        workflow_id int8,
        primary key (id)
    );

    alter table if exists compat_app_info 
       add constraint uidx01_app_info unique (app_name);
create index idx01_container_info on compat_container_info (app_id);
create index idx01_instance_info on compat_instance_info (job_id, status);
create index idx02_instance_info on compat_instance_info (app_id, status);
create index idx03_instance_info on compat_instance_info (instance_id, status);
create index idx04_instance_info_outer_key on compat_instance_info (outer_key);
create index idx01_job_info on compat_job_info (app_id, status, time_expression_type, next_trigger_time);

    alter table if exists compat_namespace 
       add constraint uidx01_namespace unique (code);

    alter table if exists compat_oms_lock 
       add constraint uidx01_oms_lock unique (lock_name);

    alter table if exists compat_pwjb_user_info 
       add constraint uidx01_username unique (username);
create index idx01_server_info on compat_server_info (gmt_modified);

    alter table if exists compat_server_info 
       add constraint uidx01_server_info unique (ip);

    alter table if exists compat_sundry 
       add constraint uidx01_sundry unique (pkey, skey);
create index uidx02_user_info on compat_user_info (email);

    alter table if exists compat_user_info 
       add constraint uidx01_user_name unique (username);
create index uidx01_user_id on compat_user_role (user_id);
create index idx01_workflow_info on compat_workflow_info (app_id, status, time_expression_type, next_trigger_time);
create index idx01_wf_instance on compat_workflow_instance_info (workflow_id, status, app_id, expected_trigger_time);

    alter table if exists compat_workflow_instance_info 
       add constraint uidx01_wf_instance unique (wf_instance_id);
create index idx01_workflow_node_info on compat_workflow_node_info (workflow_id, gmt_create);

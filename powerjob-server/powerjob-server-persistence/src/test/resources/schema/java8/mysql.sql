
    create table compat_app_info (
       id bigint not null auto_increment,
        app_name varchar(255),
        creator bigint,
        current_server varchar(255),
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        modifier bigint,
        namespace_id bigint,
        password varchar(255),
        tags varchar(255),
        title varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_container_info (
       id bigint not null auto_increment,
        app_id bigint,
        container_name varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        last_deploy_time datetime(6),
        source_info varchar(255),
        source_type integer,
        status integer,
        version varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_instance_info (
       id bigint not null auto_increment,
        actual_trigger_time bigint,
        app_id bigint,
        expected_trigger_time bigint,
        extend_value varchar(255),
        finished_time bigint,
        gmt_create datetime(6),
        gmt_modified datetime(6),
        instance_id bigint,
        instance_params longtext,
        job_id bigint,
        job_params longtext,
        last_report_time bigint,
        meta varchar(255),
        outer_key varchar(255),
        result longtext,
        running_times bigint,
        status integer,
        task_tracker_address varchar(255),
        type integer,
        wf_instance_id bigint,
        primary key (id)
    ) engine=InnoDB;

    create table compat_job_info (
       id bigint not null auto_increment,
        advanced_runtime_config varchar(255),
        alarm_config varchar(255),
        app_id bigint,
        concurrency integer,
        designated_workers varchar(255),
        dispatch_strategy integer,
        dispatch_strategy_config varchar(255),
        execute_type integer,
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        instance_retry_num integer,
        instance_time_limit bigint,
        job_description varchar(255),
        job_name varchar(255),
        job_params longtext,
        lifecycle varchar(255),
        log_config varchar(255),
        max_instance_num integer,
        max_worker_count integer,
        min_cpu_cores double precision not null,
        min_disk_space double precision not null,
        min_memory_space double precision not null,
        next_trigger_time bigint,
        notify_user_ids varchar(255),
        processor_info varchar(255),
        processor_type integer,
        status integer,
        tag varchar(255),
        task_retry_num integer,
        time_expression varchar(255),
        time_expression_type integer,
        primary key (id)
    ) engine=InnoDB;

    create table compat_namespace (
       id bigint not null auto_increment,
        code varchar(255),
        creator bigint,
        dept varchar(255),
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        modifier bigint,
        name varchar(255),
        status integer,
        tags varchar(255),
        token varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_oms_lock (
       id bigint not null auto_increment,
        gmt_create datetime(6),
        gmt_modified datetime(6),
        lock_name varchar(255),
        max_lock_time bigint,
        ownerip varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_pwjb_user_info (
       id bigint not null auto_increment,
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        password varchar(255),
        username varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_server_info (
       id bigint not null auto_increment,
        gmt_create datetime(6),
        gmt_modified datetime(6),
        ip varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_sundry (
       id bigint not null auto_increment,
        content varchar(255),
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        pkey varchar(255),
        skey varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_user_info (
       id bigint not null auto_increment,
        account_type varchar(255),
        email varchar(255),
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        nick varchar(255),
        origin_username varchar(255),
        password varchar(255),
        phone varchar(255),
        status integer,
        token_login_verify_info varchar(255),
        username varchar(255),
        web_hook varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_user_role (
       id bigint not null auto_increment,
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        role integer,
        scope integer,
        target bigint,
        user_id bigint,
        primary key (id)
    ) engine=InnoDB;

    create table compat_workflow_info (
       id bigint not null auto_increment,
        app_id bigint,
        extra varchar(255),
        gmt_create datetime(6),
        gmt_modified datetime(6),
        lifecycle varchar(255),
        max_wf_instance_num integer,
        next_trigger_time bigint,
        notify_user_ids varchar(255),
        pedag longtext,
        status integer,
        time_expression varchar(255),
        time_expression_type integer,
        wf_description varchar(255),
        wf_name varchar(255),
        primary key (id)
    ) engine=InnoDB;

    create table compat_workflow_instance_info (
       id bigint not null auto_increment,
        actual_trigger_time bigint,
        app_id bigint,
        dag longtext,
        expected_trigger_time bigint,
        finished_time bigint,
        gmt_create datetime(6),
        gmt_modified datetime(6),
        parent_wf_instance_id bigint,
        result longtext,
        status integer,
        wf_context longtext,
        wf_init_params longtext,
        wf_instance_id bigint,
        workflow_id bigint,
        primary key (id)
    ) engine=InnoDB;

    create table compat_workflow_node_info (
       id bigint not null auto_increment,
        app_id bigint not null,
        enable bit not null,
        extra longtext,
        gmt_create datetime(6) not null,
        gmt_modified datetime(6) not null,
        job_id bigint,
        node_name varchar(255),
        node_params longtext,
        skip_when_failed bit not null,
        type integer,
        workflow_id bigint,
        primary key (id)
    ) engine=InnoDB;

    alter table compat_app_info 
       add constraint uidx01_app_info unique (app_name);
create index idx01_container_info on compat_container_info (app_id);
create index idx01_instance_info on compat_instance_info (job_id, status);
create index idx02_instance_info on compat_instance_info (app_id, status);
create index idx03_instance_info on compat_instance_info (instance_id, status);
create index idx04_instance_info_outer_key on compat_instance_info (outer_key);
create index idx01_job_info on compat_job_info (app_id, status, time_expression_type, next_trigger_time);

    alter table compat_namespace 
       add constraint uidx01_namespace unique (code);

    alter table compat_oms_lock 
       add constraint uidx01_oms_lock unique (lock_name);

    alter table compat_pwjb_user_info 
       add constraint uidx01_username unique (username);
create index idx01_server_info on compat_server_info (gmt_modified);

    alter table compat_server_info 
       add constraint uidx01_server_info unique (ip);

    alter table compat_sundry 
       add constraint uidx01_sundry unique (pkey, skey);
create index uidx02_user_info on compat_user_info (email);

    alter table compat_user_info 
       add constraint uidx01_user_name unique (username);
create index uidx01_user_id on compat_user_role (user_id);
create index idx01_workflow_info on compat_workflow_info (app_id, status, time_expression_type, next_trigger_time);
create index idx01_wf_instance on compat_workflow_instance_info (workflow_id, status, app_id, expected_trigger_time);

    alter table compat_workflow_instance_info 
       add constraint uidx01_wf_instance unique (wf_instance_id);
create index idx01_workflow_node_info on compat_workflow_node_info (workflow_id, gmt_create);

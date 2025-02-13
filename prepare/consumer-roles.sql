-- create consumer role
create role if not exists chairlift_admin;
create role if not exists chairlift_viewer;
grant role chairlift_admin to role accountadmin;
grant role chairlift_viewer to role accountadmin;
grant create database on account to role chairlift_admin;
grant create application on account to role chairlift_admin;
grant execute task, execute managed task on account to role chairlift_admin with grant option;
grant role chairlift_viewer to role chairlift_admin;

-- ensure a warehouse is usable by consumer
grant usage on warehouse chairlift_wh to role chairlift_admin;
grant usage on warehouse chairlift_wh to role chairlift_viewer;

-- allow restricted application access to a secondary role
use role chairlift_admin;
use warehouse chairlift_wh;

-- allow our secondary viewer role restricted access to the app
grant application role chairlift_app.app_viewer
    to role chairlift_viewer;
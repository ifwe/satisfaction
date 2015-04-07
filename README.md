
Satisfaction
============

# Next-Generation Hadoop Scheduler

## Running

To run, run the `willrogers` play project

```
sbt
> project willrogers
> run
```

Default port is 9000

## Development

See the instructions under the `sbt-satisfy` directory

## Deployment 

To deploy, you create an RPM

```
sbt
> project willrogers
> rpm:packageBin
```

You will need to have `rpmbuild` installed on your machine.
Move the RPM file under apps/willrogers/target/rpm/RPMS/noarc to the host machine, and
install it with the `rpm` command

```
jbanks-laptop:noarch jbanks$ scp satisfaction-scheduler-2.0.1-1.noarch.rpm jbanks@dhdp2jump01:
jbanks-laptop:~ jbanks$ ssh jbanks@dhdp2jump01
[jbanks@dhdp2jump01 ~]$ sudo rpm -i satisfaction-scheduler-2.0.1-1.noarch.rpm 
....

```


Alternatively, you can run `dist`, and produce a single large zipfile.







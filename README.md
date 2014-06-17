
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

See the instructions under the `sbt-deploy` directory

## Deployment 

To deploy, you create an RPM

```
sbt
> project willrogers
> rpm:packageBin
```

You will need to have `rpmbuild` installed on your machine.

Alternatively, you can run `dist`, and produce a single large zipfile.







# Alpian fork of confluentinc/schema-registry

This fork exists to republish two Confluent artifacts with PII handling compiled in, for
services that cannot get rules or metadata from the schema registry:

| artifact | what is patched |
|---|---|
| `kafka-protobuf-serializer` | a PII redaction rule set compiled into the deserializer |
| `kafka-connect-protobuf-converter` | a field's `PII` meta tag is carried into the Connect schema as a `PII=true` parameter |

Everything else in the tree is upstream, untouched.

## The branch model

| branch | what it is |
|---|---|
| `alpian/main` | An upstream release tag plus the Alpian commits. What CI builds and publishes, and the branch that should be this fork's **default**. |
| `master` | a mirror of upstream `master`. Not built — see *Why not master* below. |

### One-time setup

```bash
git push -u origin alpian/main
```

then **Settings → General → Default branch → `alpian/main`**. That second step is not cosmetic:
GitHub only offers a workflow's *Run workflow* button when the workflow file is present on the
default branch, so until it is switched the manual version-override input is unreachable and only
the `push` trigger fires.

`.alpian-base` records the upstream tag `alpian/main` is currently based on. CI cross-checks
three things that must agree — that file, the reactor version (which comes from the tag itself,
and so is the one that cannot be forgotten), and each patched module pom's `<version>` — and
fails the build on any mismatch. A rebase that restamps none of them, or only some, cannot
publish an artifact whose name misstates what it was built from. The two patched modules are
versioned independently of each other; the check runs per module.

The Alpian change is deliberately small, and confined to two modules:

```
protobuf-serializer/src/main/java/io/confluent/kafka/serializers/protobuf/
    AbstractKafkaProtobufDeserializer.java   (modified)  attaches the rule set on the read path
    KafkaProtobufDeserializerConfig.java     (modified)  adds hardcoded.rule.set.enable
    HardcodedRuleSets.java                   (new)       the rule set itself
protobuf-serializer/src/test/java/io/confluent/kafka/serializers/protobuf/
    KafkaProtobufDeserializerHardcodedRuleSetTest.java  (new)
protobuf-serializer/pom.xml                  (modified)  version override

protobuf-converter/src/main/java/io/confluent/connect/protobuf/
    ProtobufData.java                        (modified)  PII meta tag -> Connect schema parameter
protobuf-converter/src/test/java/io/confluent/connect/protobuf/
    ProtobufDataPiiTagTest.java              (new)
protobuf-converter/pom.xml                   (modified)  version override
```

Small on purpose: the smaller the diff, the less there is to conflict when the base moves. The
`ProtobufData` change is a single block at the end of `toConnectSchema(ToConnectContext,
FieldDescriptor)`, immediately before the existing `PROTOBUF_TYPE_TAG` parameter — a stable
landmark to re-find if that method ever moves.

## Moving to a newer upstream release

One rebase. Git reports conflicts as conflicts — there is no patch file to apply by hand and
nothing that can silently half-succeed.

```bash
# 1. Fetch just the tag you want. --no-tags is configured on the remote because upstream has
#    ~31,000 of them (they tag every CI build) and you want none of the rest.
git remote add upstream https://github.com/confluentinc/schema-registry.git   # once
git config remote.upstream.tagOpt --no-tags                                   # once
git fetch upstream 'refs/tags/v8.4.0:refs/tags/v8.4.0'

# 2. Replay the Alpian commits from the old base onto the new one.
git switch alpian/main
git rebase --onto v8.4.0 "$(cat .alpian-base)"

# 3. Restamp the three places that name the base.
echo v8.4.0 > .alpian-base
#    ...and the <version> in BOTH protobuf-serializer/pom.xml and protobuf-converter/pom.xml
#    -> 8.4.0-alpian-1. The prefix has to track the base even though the two qualifiers are
#    otherwise independent, so this is the one time both poms move together.

# 4. Verify locally, exactly as CI does.
mvn -B -pl protobuf-serializer,protobuf-converter \
    -Dspotless.check.skip=true -Dcheckstyle.skip=true verify

git commit -am "Rebase onto v8.4.0"
git push --force-with-lease origin alpian/main
```

The force-push is expected: rebasing rewrites the branch. `--force-with-lease` is what stops
it clobbering someone else's push.

### How much drift to expect

Different for the two modules.

**`protobuf-serializer`: none so far.** The three upstream files that patch depends on —
`AbstractKafkaProtobufDeserializer.java`, `KafkaProtobufDeserializerConfig.java` and
`schema-serializer`'s `AbstractKafkaSchemaSerDe.java` — were **byte-identical from `v8.3.1`
through upstream `master` (the 8.5.0-0 line)**, i.e. unchanged across six minor releases.

**`protobuf-converter`: some, but not where it matters.** `ProtobufData.java` does move
upstream — 33 hunks between `v8.3.1` and `master`, mostly protobuf-wrapper handling in
`fromConnectData`. None of them touch
`toConnectSchema(ToConnectContext, FieldDescriptor)`, so expect the patch block itself to
replay cleanly. The one line to watch is the `PII_TAG` constant: upstream `master` adds its
own `PROTOBUF_TYPE_WRAPPER` constant in the same spot, so rebasing past 8.3.1 will likely
produce one adjacent-line conflict there. Keep both constants; there is no semantic clash.

So the realistic maintenance event is still wanting a newer base, not upstream breaking the
patch.

### Why not `master`

Upstream `master` targets the 8.5.0-0 line, whose parent POM
(`io.confluent:rest-utils-parent:8.5.0-0`) is published only inside Confluent —
packages.confluent.io stops at 8.3.1. Building it means building `common` and `rest-utils` from
source first. Since the files this patch touches are identical between 8.3.1 and master, basing
on the newest *public* tag gets the same code with none of that.

Pick the base by checking what actually resolves:

```bash
curl -s https://packages.confluent.io/maven/io/confluent/rest-utils-parent/maven-metadata.xml \
  | grep -o '<release>[^<]*'
```

## Building and publishing

CI (`.github/workflows/publish-alpian-modules.yml`) runs on every push to `alpian/main` that
touches either patched module, and can be run by hand from the Actions tab with an optional
version override. Pull requests build and test but do not publish. The two modules run as a
matrix with `fail-fast: false` — they publish to distinct coordinates, so one failing job
should not cancel the other's result.

**A run publishes only the modules the push actually touched.** A `select` job diffs the push
range and feeds the matrix; a change to `.alpian-base` or to the workflow itself builds both,
since either affects both. That is not tidiness — GitHub Packages refuses to overwrite an
existing release version, so rebuilding an unchanged module would fail the job on a 409. Pull
requests build both regardless, because a change to one module can still break the other's
tests, and a PR never publishes so there is nothing to collide with.

The manual dispatch takes a module (`both` by default) and an optional version override. The
override is rejected together with `both`: the two modules are versioned independently, and
stamping one number onto both is never what you meant.

Only those two modules are built. The sibling artifacts resolve from packages.confluent.io as
published jars, which is why there is no `-am` and no second repository to clone.

Locally:

```bash
mvn -pl protobuf-serializer,protobuf-converter \
    -Dspotless.check.skip=true -Dcheckstyle.skip=true verify
mvn -pl protobuf-serializer,protobuf-converter -DskipTests install   # into ~/.m2
```

> **A build leaves the tree dirty — in both modules.** `protobuf-maven-plugin` regenerates the
> checked-in test sources, and the output differs from upstream's unless your `protoc` matches
> theirs exactly. That is harmless noise, but never commit it — `git add -A` after a build will
> sweep in ~34 files and bury the actual change. Add only the paths you edited, or discard them
> first:
>
> ```bash
> git restore protobuf-serializer/src/test/java/com/acme/glup \
>             protobuf-serializer/src/test/java/io/confluent/connect/protobuf/test \
>             protobuf-serializer/src/test/java/io/confluent/kafka/serializers/protobuf/test \
>             protobuf-converter/src/test/java/io/confluent/connect/protobuf/test
> ```

### Coordinates

Published as:

```
io.confluent:kafka-protobuf-serializer:8.3.1-alpian-1
io.confluent:kafka-connect-protobuf-converter:8.3.1-alpian-1
```

**Confluent's groupId and artifactId, deliberately.** A consumer pulls these in alongside
`io.confluent:*` siblings, and only matching `groupId:artifactId` lets Maven and Gradle treat
ours and upstream's as one dependency and resolve a single version. Republishing under
`com.alpian` would put both jars on the classpath carrying the same class names, and which one
won would come down to classpath order.

**The two qualifiers are independent.** They both read `-alpian-1` today only because both
patches happen to be at their first release. Bump the one whose module changed and leave the
other alone: republishing a byte-identical jar under a new number would make every consumer edit
a pin for a rebuild they did not need. Expect them to drift apart, and do not read a matching
qualifier as meaning the two were released together.

The prefix is the exception. It names the upstream base, so a rebase moves it on both at once.

The cost is that a consumer must point at GitHub Packages for an `io.confluent` artifact, so a
repository content filter has to allow it. In Gradle:

```groovy
repositories {
    maven {
        url = "https://maven.pkg.github.com/alpian-swiss/schema-registry"
        credentials {
            username = System.getenv('GITHUB_ACTOR')
            password = System.getenv('GITHUB_TOKEN')
        }
        content {
            // Narrower than includeGroupByRegex "com\\.alpian.*" -- these two artifacts only,
            // so every other io.confluent dependency still comes from Confluent's repo.
            includeModule('io.confluent', 'kafka-protobuf-serializer')
            includeModule('io.confluent', 'kafka-connect-protobuf-converter')
        }
    }
}
```

Keep Confluent's own repository declared alongside it. The filter above deliberately lets only
this one artifact come from GitHub Packages, and the rest of the chain still has to resolve from
somewhere: this jar's POM inherits
`io.confluent:kafka-schema-registry-parent:8.3.1` → `rest-utils-parent` → `common-parent`, and its
`kafka-protobuf-types` dependency carries no version of its own, so it comes from that parent's
`dependencyManagement`. All three parent POMs are public:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-parent/8.3.1/kafka-schema-registry-parent-8.3.1.pom
```

`kafka-connect-protobuf-converter` resolves the same way, with one extra note: its POM depends on
the **upstream** `io.confluent:kafka-protobuf-serializer:8.3.1`, not on our patched build. That
is on purpose — pointing it at `8.3.1-alpian-1` would make CI's two jobs depend on each other's
publish order, and would have to be restamped every time the serializer's qualifier moved. If a Connect worker needs both the schema parameter and the redaction, pin both
artifacts explicitly as below; the version constraint is what makes the patched serializer win
over the one the converter's POM asks for.

If the service resolves through an internal mirror rather than packages.confluent.io directly,
check that the mirror actually carries the 8.3.1 line — one pinned to a different release will
fail to resolve the parent, not the jar, which makes for a confusing error.

Then pin the version so nothing drags the upstream one back in:

```groovy
dependencies {
    implementation('io.confluent:kafka-protobuf-serializer:8.3.1-alpian-1')
    implementation('io.confluent:kafka-connect-protobuf-converter:8.3.1-alpian-1')
    constraints {
        implementation('io.confluent:kafka-protobuf-serializer:8.3.1-alpian-1') {
            because 'carries the hardcoded PII redaction rule set'
        }
        implementation('io.confluent:kafka-connect-protobuf-converter:8.3.1-alpian-1') {
            because 'carries the PII meta tag into the Connect schema'
        }
    }
}
```

Keep the other `io.confluent:*` dependencies in that service on the same base version (8.3.1)
rather than mixing with 8.5.x.

## Consumer configuration

### Redaction (`kafka-protobuf-serializer`)

The rule set is off unless asked for. Three properties travel together:

```properties
hardcoded.rule.set.enable   = true
rule.executors              = redact
rule.executors.redact.class = io.confluent.kafka.schemaregistry.rules.FieldRedactionExecutor
```

`FieldRedactionExecutor` ships in `kafka-schema-registry-client`, so there is no extra artifact
to add — but it is not registered through the `ServiceLoader`, so it has to be named. Omit those
last two and the rule fails the deserialize with *"Could not find rule executor of type
REDACT"*. Failing closed is the right default for a masking rule; it does mean all three
properties are set together or none.

The rule matches on tags, and with no registry metadata to carry them the only live source is
the proto itself:

```protobuf
import "confluent/meta.proto";

message Customer {
  string email = 1 [(.confluent.field_meta).tags = "PII"];
  bytes  scan  = 2 [(.confluent.field_meta).tags = "PII"];
}
```

A schema with no such option matches no field and the rule does nothing.

`REDACT` replaces strings with `<REDACTED>` and bytes with those same bytes, and returns every
other type **unchanged** rather than failing. That is why it is preferred here over a
`CEL_FIELD` rule — see the class comment on `HardcodedRuleSets` for the full reasoning, but in
short: a CEL literal cannot branch on field type, several type-guarded CEL rules sharing a tag
suppress each other down to one, and a literal of the wrong type fails the record outright. A
`bool` cannot be meaningfully redacted by anything — `false` is indistinguishable from a real
value.

A schema that arrives carrying its own domain read rules keeps them; the compiled-in rule only
fills a gap. So enabling the flag cannot drop a registry-provided decryption rule for someone
who later does get rules from the registry.

### PII schema parameter (`kafka-connect-protobuf-converter`)

**Nothing to configure — it is unconditional.** Drop the jar in and every field whose Confluent
field meta carries the `PII` tag arrives with a `PII=true` schema parameter attached, on the same
schema and by the same mechanism as the `io.confluent.protobuf.Tag` parameter the converter
already sets. It reads the same proto option the redaction rule matches on, so one annotation
drives both:

```protobuf
import "confluent/meta.proto";

message Customer {
  string email = 1 [(.confluent.field_meta).tags = "PII"];
}
```

Downstream, an SMT or sink connector reads it off the field schema:

```java
if ("true".equals(field.schema().parameters().get("PII"))) { ... }
```

Four things are worth knowing before relying on it:

- **The tag need not be first.** A field tagged `["INTERNAL", "PII"]` is matched; the check is
  membership, not position.
- **Every field type is marked, not just the maskable ones.** A tag is a classification, not a
  redaction instruction, so an `int64` or a `bool` tagged `PII` gets the parameter even though
  `REDACT` would leave its value untouched. Do not read `PII=true` on the Connect side as "this
  value has already been masked".
- **On a repeated field the parameter is on the array schema, not the element.** Same placement
  as the existing tag-number parameter. `field.schema().parameters()` is the right place to
  look either way; `field.schema().valueSchema().parameters()` is not.
- **It does not survive a round trip back to Protobuf.** `fromConnectSchema` only reads the
  parameters upstream defines, so a Connect schema carrying `PII=true` converts back to a proto
  without re-emitting the field meta option. The proto is the source of truth; the parameter is
  a one-way projection of it onto the Connect side.

An untagged schema gets no parameters and behaves exactly as upstream.

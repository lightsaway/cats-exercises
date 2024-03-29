import cats.{ApplicativeError, NonEmptyTraverse}
import cats.effect.IOApp

object RaceExercise extends IOApp {

  import cats.Reducible
  import scala.util.Random
  import scala.concurrent.duration._
  import cats.data._
  import cats.implicits._
  import cats.effect.{IO, Timer, Concurrent, ExitCase, ExitCode}

  case class Data(source: String, body: String)

  def provider(name: String)(implicit timer: Timer[IO]): IO[Data] = {
    val proc = for {
      dur <- IO { Random.nextInt(500) }
      _   <- IO.sleep { (100 + dur).millis }
      _   <- IO { if (Random.nextBoolean()) throw new Exception(s"Error in $name") }
      txt <- IO { Random.alphanumeric.take(16).mkString }
    } yield Data(name, txt)

    proc.guaranteeCase {
      case ExitCase.Completed => IO { println(s"$name request finished") }
      case ExitCase.Canceled  => IO { println(s"$name request canceled") }
      case ExitCase.Error(ex) => IO { println(s"$name errored") }
    }
  }

  case class CompositeException(ex: NonEmptyList[Throwable])
      extends Exception("All race candidates have failed")

  def raceToSuccess[F[_], C[_], A](ios: C[F[A]])(implicit C: NonEmptyTraverse[C],
                                                 F: Concurrent[F]): F[A] = {
    C.map(ios)(_.attempt.map(_.leftMap(e => CompositeException(NonEmptyList.one(e)))))
      .reduce {
        case (a, b) => {
          F.racePair(a, b).flatMap {
            case Left((Right(res), f)) => f.cancel.as(Right(res))
            case Left((Left(error), f)) =>
              f.join.map(e => e.left.map(er => CompositeException(er.ex ::: error.ex)))
            case Right((f, Right(res))) => f.cancel.as(Right(res))
            case Right((f, Left(error))) =>
              f.join.map(e => e.left.map(er => CompositeException(er.ex ::: error.ex)))
          }
        }
      }
      .rethrow
  }

  // In your IOApp, you can use the following sample method list

  val methods: NonEmptyList[IO[Data]] = NonEmptyList
    .of(
      "memcached",
      "redis",
      "postgres",
      "mongodb",
      "hdd",
      "aws"
    )
    .map(provider)

  def run(args: List[String]): IO[ExitCode] = {
    def oneRace =
      raceToSuccess[IO, NonEmptyList, Data](methods)
        .flatMap(a => IO(println(s"Final result is $a")))
        .handleErrorWith(err => IO(err.printStackTrace()))

    oneRace.replicateA(5).as(ExitCode.Success)
  }
}

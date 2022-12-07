package com.snz1.jdbc.rest;

import java.io.PrintStream;

import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.ansi.AnsiColor;
import org.springframework.boot.ansi.AnsiOutput;
import org.springframework.boot.ansi.AnsiStyle;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.env.Environment;

/**
 * 应用启动类
 */

@EnableAspectJAutoProxy(proxyTargetClass = true)
@EnableAutoConfiguration
@SpringBootApplication
@com.snz1.annotation.Snz1dpApplication //启用Snz1p应用配置
@com.snz1.annotation.EnableAutoCaching //启用默认缓存配置
@com.snz1.annotation.EnableWebMvc // 启用Mvc默认配置
@com.snz1.annotation.EnableMyBatis // 启用MyBatis默认配置
public class Application {

  public static void main(String[] args) {
    SpringApplication app = new SpringApplication(Application.class);
    app.setBanner(new ApplicationBanner());
    app.run(args);
  }

  private static class ApplicationBanner implements Banner {

    // figlet JdbcRest |sed -e "s/\\\\/\\\\\\\\/g" | sed -e "s/^/\"/" | sed -e "s/\$/\", \/\//g"
    private static final String[] BANNER = { //
      "     _     _ _          ____           _   ", //
      "    | | __| | |__   ___|  _ \\ ___  ___| |_ ", //
      " _  | |/ _` | '_ \\ / __| |_) / _ \\/ __| __|", //
      "| |_| | (_| | |_) | (__|  _ <  __/\\__ \\ |_ ", //
      " \\___/ \\__,_|_.__/ \\___|_| \\_\\___||___/\\__|", //
      "                                           ", //
    };

    private static final String APP_BOOT = ":: Snz1DP :: ";

    private static final int STRAP_LINE_SIZE = 16;

    @Override
    public void printBanner(Environment environment, Class<?> sourceClass, PrintStream printStream) {

      for (String line : BANNER) {
        printStream.println(line);
      }
      String version = Version.VERSION;
      version = (version == null ? "" : " v" + version + "");
      String padding = "";
      while (padding.length() < STRAP_LINE_SIZE) {
        padding += " ";
      }

      printStream.println(AnsiOutput.toString(AnsiColor.GREEN, APP_BOOT,
        AnsiColor.DEFAULT, padding, AnsiStyle.FAINT, version));

      printStream.println();
    }

  }

}

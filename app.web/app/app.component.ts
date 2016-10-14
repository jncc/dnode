
import { Component } from '@angular/core'

@Component({
  selector: 'my-app',
  styleUrls: [ 'app.component.css' ],
  moduleId: module.id,
  template: `
      <h1>{{title}}</h1>
      <nav>
        <a routerLink="/dashboard">Dashboard</a>
        <a routerLink="/heroes">Heroes</a>
      </nav>
      <router-outlet></router-outlet>
    `
})
export class AppComponent {
  title = 'Go the distance'
}

